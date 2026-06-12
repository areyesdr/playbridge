#!/bin/bash
# Debian 12: setup del sincronizador
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
echo ""
echo "✅ Listo. Lanza con:  source venv/bin/activate && python3 app.py"
echo "   PC:      http://localhost:5000"
echo "   Android: http://$(hostname -I | awk '{print $1}'):5000"
