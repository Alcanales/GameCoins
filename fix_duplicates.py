import logging
import json
import datetime
from sqlalchemy import or_
from database import SessionLocal
from models import GameCoinUser

# Configuración
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger("TotalCleaner")

def respaldar_datos(session):
    """Guarda una copia de seguridad en JSON antes de hacer cambios."""
    logger.info("💾 Generando respaldo de seguridad...")
    users = session.query(GameCoinUser).all()
    data = []
    for u in users:
        data.append({
            "id": u.id, "email": u.email, "rut": u.rut,
            "name": u.name, "surname": u.surname, "saldo": u.saldo
        })
    
    filename = f"backup_antes_de_limpiar_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    logger.info(f"✅ Respaldo guardado en: {filename}")

def normalizar_usuario(u):
    """Limpia los textos del usuario."""
    cambios = False
    
    # Email: Minúsculas y sin espacios
    if u.email:
        clean_email = u.email.strip().lower()
        if u.email != clean_email:
            u.email = clean_email
            cambios = True
            
    # Nombre: Title Case (Juan Perez)
    if u.name:
        clean_name = u.name.strip().title()
        if u.name != clean_name:
            u.name = clean_name
            cambios = True
            
    # Apellido: Title Case
    if u.surname:
        clean_surname = u.surname.strip().title()
        if u.surname != clean_surname:
            u.surname = clean_surname
            cambios = True
            
    # RUT: Mayúsculas y sin espacios
    if u.rut:
        clean_rut = u.rut.strip().upper()
        if u.rut != clean_rut:
            u.rut = clean_rut
            cambios = True
            
    return cambios

def puntaje_calidad(user):
    """Define qué usuario vale más la pena conservar."""
    score = 0
    if user.rut and len(user.rut) > 3 and "MAN-" not in user.rut and "PENDIENTE" not in user.rut: score += 100
    if user.name and user.name.lower() != "cliente": score += 50
    if user.saldo > 0: score += 1000 # ¡El saldo es sagrado!
    score += (user.id * 0.01) # Preferencia por el ID más alto (más nuevo) en empate
    return score

def ejecutar_limpieza_total():
    session = SessionLocal()
    try:
        print("==========================================")
        print("   🚀 INICIANDO PROTOCOLO DE LIMPIEZA TOTAL")
        print("==========================================")
        
        # 1. RESPALDO
        respaldar_datos(session)
        
        # 2. NORMALIZACIÓN
        logger.info("🛠️  Normalizando textos (Mayúsculas/Minúsculas)...")
        users = session.query(GameCoinUser).all()
        norm_count = 0
        for u in users:
            if normalizar_usuario(u):
                norm_count += 1
        session.commit() # Guardamos normalización para que el agrupamiento funcione bien
        logger.info(f"✅ {norm_count} usuarios normalizados.")

        # 3. FUSIÓN DE DUPLICADOS (Recargamos usuarios ya normalizados)
        users = session.query(GameCoinUser).all()
        grupos = {}
        for u in users:
            email = u.email # Ya está en minúsculas
            if email not in grupos: grupos[email] = []
            grupos[email].append(u)
            
        fusionados = 0
        eliminados_fusion = 0
        
        logger.info("🔄 Buscando y fusionando duplicados...")
        for email, lista in grupos.items():
            if len(lista) < 2: continue
            
            lista.sort(key=puntaje_calidad, reverse=True)
            maestro = lista[0]
            duplicados = lista[1:]
            
            saldo_extra = 0
            for dup in duplicados:
                saldo_extra += dup.saldo
                # Rescatar datos útiles
                if (not maestro.rut or "MAN-" in maestro.rut) and (dup.rut and "MAN-" not in dup.rut):
                    maestro.rut = dup.rut
                if (not maestro.surname) and dup.surname:
                    maestro.surname = dup.surname
                
                session.delete(dup)
                eliminados_fusion += 1
            
            if saldo_extra > 0:
                maestro.saldo += saldo_extra
                logger.info(f"   💰 {email}: Fusionado saldo +${saldo_extra}. Total: ${maestro.saldo}")
            
            fusionados += 1
            
        session.commit()
        logger.info(f"✅ Se fusionaron {fusionados} grupos (Total eliminados: {eliminados_fusion})")

        # 4. PURGA DE ZOMBIES (Usuarios vacíos sin saldo)
        logger.info("💀 Buscando usuarios 'Zombie' (Sin saldo y sin datos)...")
        
        # Criterio de Zombie: Saldo 0 Y (Rut 'MAN-' o 'PENDIENTE') Y (Nombre 'Cliente' o vacío)
        zombies = session.query(GameCoinUser).filter(
            GameCoinUser.saldo == 0,
            or_(GameCoinUser.rut.like("MAN-%"), GameCoinUser.rut.like("PENDIENTE%"), GameCoinUser.rut == ""),
            or_(GameCoinUser.name == "Cliente", GameCoinUser.name == "")
        ).all()
        
        zombies_count = len(zombies)
        for z in zombies:
            session.delete(z)
            
        session.commit()
        logger.info(f"✅ Se eliminaron {zombies_count} usuarios basura (Zombies).")
        
        print("==========================================")
        print("   ✨ LIMPIEZA TOTAL FINALIZADA CON ÉXITO")
        print("==========================================")

    except Exception as e:
        session.rollback()
        logger.error(f"❌ ERROR CRÍTICO: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    ejecutar_limpieza_total()
