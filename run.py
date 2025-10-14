import uvicorn
from dotenv import load_dotenv

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

if __name__ == "__main__":
    # Inicia o servidor web SEM o recarregamento automático
    uvicorn.run("app.main:app", host="0.0.0.0", port=1337)
