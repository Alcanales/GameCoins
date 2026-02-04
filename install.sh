#!/bin/bash
# install.sh - Script de instalación optimizado para Render
# Uso: chmod +x install.sh && ./install.sh

set -e  # Exit on error

echo "🚀 Instalando dependencias de GameQuest API..."

# Actualizar pip
pip install --upgrade pip setuptools wheel

# Instalar dependencias de producción
echo "📦 Instalando dependencias principales..."
pip install --no-cache-dir -r requirements.txt

# Verificar instalación crítica
echo "✅ Verificando instalaciones críticas..."
python -c "import fastapi; import sqlalchemy; import jose; import passlib; print('✓ Dependencias críticas OK')"

echo "🎉 Instalación completada exitosamente!"
