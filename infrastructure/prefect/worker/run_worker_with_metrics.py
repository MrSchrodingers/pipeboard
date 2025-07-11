import os
import subprocess
import time
import signal
import structlog
from prometheus_client import start_http_server

log = structlog.get_logger(__name__)

APP_METRICS_PORT = int(os.environ.get("APP_METRICS_PORT", 8082))
PREFECT_WORK_POOL_NAME = os.environ.get("PREFECT_WORK_POOL_NAME", "default-agent-pool") 
PREFECT_API_URL = os.environ.get("PREFECT_API_URL")

worker_process_instance = None

def handle_signal(signum, frame):
    log.warn(f"Sinal {signal.Signals(signum).name if isinstance(signum, signal.Signals) else signum} recebido, encerrando...")
    if worker_process_instance and worker_process_instance.poll() is None:
        log.info("Terminando processo do worker do Prefect...")
        worker_process_instance.terminate()
        try:
            worker_process_instance.wait(timeout=10) # Dê 10 segundos para terminar
            log.info("Processo do worker do Prefect terminado.")
        except subprocess.TimeoutExpired:
            log.warn("Worker do Prefect não encerrou a tempo, forçando kill.")
            worker_process_instance.kill()
    log.info("Processo principal de run_worker_with_metrics.py encerrado.")
    exit(0)

if __name__ == "__main__":
    if not PREFECT_API_URL:
        log.error("Variável de ambiente PREFECT_API_URL não definida. Encerrando.")
        exit(1)
    if not PREFECT_WORK_POOL_NAME:
        log.error("Variável de ambiente PREFECT_WORK_POOL_NAME não definida. Encerrando.")
        exit(1)

    log.info(f"Iniciando servidor de métricas Prometheus na porta {APP_METRICS_PORT}...")
    try:
        start_http_server(APP_METRICS_PORT)
        log.info(f"Servidor de métricas Prometheus iniciado com sucesso na porta {APP_METRICS_PORT}.")
    except Exception as e_metrics:
        log.error("Falha ao iniciar o servidor de métricas Prometheus.", error=str(e_metrics), exc_info=True)
        # Decida se o worker deve falhar se o servidor de métricas não iniciar
        # exit(1) # Ou apenas logue e continue para que o Pushgateway ainda possa funcionar

    # Configura manipuladores de sinal para desligamento gracioso
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # Espera a API do Prefect ficar saudável (lógica do seu worker_entrypoint.sh)
    log.info(f"⏳ Esperando Prefect API em {PREFECT_API_URL}…")
    health_check_url = f"{PREFECT_API_URL.rstrip('/api')}/health"
    max_retries = 24 # Tenta por 2 minutos (24 * 5s)
    retries = 0
    while retries < max_retries:
        try:
            # Usando subprocess para curl para evitar adicionar 'requests' como dependência aqui
            # ou use a biblioteca http.client se preferir não usar curl.
            curl_cmd = ["curl", "-sf", health_check_url]
            process = subprocess.run(curl_cmd, capture_output=True, text=True, timeout=5)
            if process.returncode == 0:
                log.info("✅ Prefect API está pronta!")
                break
            else:
                log.warn(f"  Prefect API ainda não disponível (saída curl: {process.stderr.strip()}), tentando de novo em 5s… ({health_check_url})")
        except subprocess.TimeoutExpired:
            log.warn(f"  Timeout ao checar Prefect API, tentando de novo em 5s… ({health_check_url})")
        except Exception as e_curl:
            log.warn(f"  Erro ao checar Prefect API ({e_curl}), tentando de novo em 5s… ({health_check_url})")
        
        retries += 1
        time.sleep(5)
    else:
        log.error(f"Prefect API em {health_check_url} não ficou saudável após {max_retries} tentativas. Encerrando.")
        exit(1)

    worker_command = ["prefect", "worker", "start", "--pool", PREFECT_WORK_POOL_NAME]
    log.info(f"Iniciando worker do Prefect: {' '.join(worker_command)}")
    
    worker_process_instance = subprocess.Popen(worker_command)
    log.info(f"Worker do Prefect iniciado com PID: {worker_process_instance.pid}")

    try:
        # Mantém o processo principal vivo e atualizando o uptime
        while True:
            if worker_process_instance.poll() is not None:
                log.error(f"Worker do Prefect encerrou inesperadamente com código {worker_process_instance.poll()}. Finalizando.")
                break
            time.sleep(10)
    except KeyboardInterrupt:
        log.info("Interrupção de teclado recebida.")
    except Exception as e_main:
        log.error("Erro no loop principal.", error=str(e_main), exc_info=True)
    finally:
        # Chama o handler de sinal para garantir o cleanup do subprocesso
        handle_signal(signal.SIGTERM, None)