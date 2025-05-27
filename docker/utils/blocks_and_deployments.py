try:
    import orchestration.plugins
    print("\n✅ Deployments dos plugins processados (auto-registrados na importação via orchestration.plugins).")
except ImportError as e:
    print(f"\n❌ Erro crítico: Falha ao importar 'orchestration.plugins'. Verifique os caminhos e arquivos __init__.py.")
    print(f"   Detalhe do erro: {e}")
    raise
except Exception as e:
    print(f"\n❌ Erro inesperado durante o processamento dos deployments (importação de orchestration.plugins): {e}")
    raise

print("🔧 Certifique-se de que o worker Prefect está escutando o work pool correto.")