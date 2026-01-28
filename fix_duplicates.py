import logging
from sqlalchemy import text
from database import SessionLocal
from models import GameCoinUser

# Configuración de Logging para ver qué pasa en la consola
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger("FixDuplicates")

def es_perfil_valido(user):
    """Retorna un puntaje de calidad del perfil."""
    score = 0
    # Preferimos perfiles con nombre real (no vacio, no default)
    if user.name and user.name.lower() != "cliente" and len(user.name) > 2:
        score += 10
    # Preferimos perfiles con RUT válido
    if user.rut and len(user.rut) > 5 and "MAN-" not in user.rut:
        score += 5
    # En caso de empate, preferimos el que tenga saldo
    if user.saldo > 0:
        score += 2
    # Finalmente, preferimos el más reciente
    score += (user.id * 0.0001) 
    return score

def fusionar_duplicados():
    session = SessionLocal()
    try:
        logger.info("🧹 Iniciando limpieza de duplicados...")
        
        # 1. Obtener todos los usuarios
        all_users = session.query(GameCoinUser).all()
        logger.info(f"Total usuarios analizados: {len(all_users)}")

        # 2. Agrupar por Email Normalizado
        grupos = {}
        for user in all_users:
            # Normalización agresiva: minúsculas y sin espacios
            clean_email = user.email.strip().lower()
            
            # FIX PARA CASOS COMO 'gmailcom' (sin punto o arroba)
            # Esto ayuda a agrupar correos mal escritos si son muy obvios, 
            # pero para seguridad, agrupamos por el string exacto normalizado.
            
            if clean_email not in grupos:
                grupos[clean_email] = []
            grupos[clean_email].append(user)

        count_fusionados = 0
        
        # 3. Procesar Grupos
        for email_key, usuarios in grupos.items():
            if len(usuarios) < 2:
                continue # No hay duplicados aquí

            # Ordenar usuarios por "Calidad" (El mejor va primero)
            usuarios.sort(key=es_perfil_valido, reverse=True)
            
            maestro = usuarios[0]
            esclavos = usuarios[1:]
            
            logger.info(f"🔄 Fusionando {email_key} ({len(usuarios)} registros)")
            logger.info(f"   👑 Maestro ID: {maestro.id} | Nombre: {maestro.name} | Saldo: ${maestro.saldo}")

            saldo_total_esclavos = 0
            
            for esclavo in esclavos:
                logger.info(f"   🗑️ Eliminando ID: {esclavo.id} | Nombre: {esclavo.name} | Saldo: ${esclavo.saldo}")
                
                # FUSIONAR SALDO: Sumamos el dinero del duplicado al maestro
                saldo_total_esclavos += esclavo.saldo
                
                # FUSIONAR DATOS: Si al maestro le falta info y el duplicado la tiene
                if (not maestro.rut or len(maestro.rut) < 4) and (esclavo.rut and len(esclavo.rut) > 4):
                    maestro.rut = esclavo.rut
                    logger.info("      -> RUT rescatado del duplicado")
                
                if (not maestro.surname) and esclavo.surname:
                    maestro.surname = esclavo.surname
                    logger.info("      -> Apellido rescatado del duplicado")

                # Borrar al duplicado
                session.delete(esclavo)
            
            # Actualizar saldo del maestro
            if saldo_total_esclavos > 0:
                maestro.saldo += saldo_total_esclavos
                logger.info(f"   💰 Nuevo saldo maestro: ${maestro.saldo}")
            
            count_fusionados += 1

        session.commit()
        logger.info(f"✅ Proceso terminado. Se fusionaron {count_fusionados} grupos de usuarios.")

    except Exception as e:
        session.rollback()
        logger.error(f"❌ Error crítico: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    # Confirmación de seguridad
    print("⚠️  ADVERTENCIA: Este script modificará la base de datos fusionando usuarios.")
    print("    Se eliminarán registros duplicados y se sumarán sus saldos.")
    
    # En Render (no interactivo) o local
    fusionar_duplicados()
