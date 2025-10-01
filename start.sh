#!/bin/bash
# Get the absolute path of the script's directory
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Activate the virtual environment
source "$DIR/.venv/bin/activate"

# Change to the script's directory to ensure correct relative paths
cd "$DIR"

# Attempt to pull the latest changes
echo "Tentando atualizar o repositório..."
if git pull; then
    echo "Repositório atualizado com sucesso."
else
    echo "AVISO: Falha ao atualizar o repositório. Usando a versão local."
fi

# Load environment variables from .env file if it exists
if [ -f .env ]; then
  echo "Loading environment variables from .env file..."
  set -a
  source .env
  set +a
fi

# Run the application using the python from the virtual environment
echo "Starting Spark Web App..."
exec "$DIR/.venv/bin/python" "$DIR/run.py"
