import uvicorn

if __name__ == "__main__":
    # Inicia o servidor web SEM o recarregamento automático
    uvicorn.run("app.main:app", host="0.0.0.0", port=1337)
