import logging
from sqlalchemy import text
from database import SessionLocal
from models import GameCoinUser

# Configuración de Logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger("WipeData")

def borrar_todo_absolutamente():
    session = SessionLocal()
    try:
        print("==============================================")
        print("   ☢️  ALERTA: PROTOCOLO DE BORRADO TOTAL ☢️")
        print("==============================================")
        print("Estás a punto de ELIMINAR TODA la tabla de usuarios.")
        print("Se perderán todos los saldos y cuentas.")
        print("==============================================")
        
        # Confirmación de seguridad (Solo funciona si corres en local interactivamente)
        # En Render (Shell) saltaremos esto o asumiremos que sabes lo que haces.
        # confirm = input("Escribe 'BORRAR' para confirmar: ")
        # if confirm != "BORRAR":
        #    print("Operación cancelada.")
        #    return

        logger.info("🗑️  Iniciando vaciado de tabla 'gamecoins'...")
        
        # Opción 1: Delete SQLAlchemy (Más seguro, compatible con todos los motores)
        num_borrados = session.query(GameCoinUser).delete()
        
        # Opción 2: Truncate (Más rápido, pero depende del motor de DB)
        # session.execute(text("TRUNCATE TABLE gamecoins RESTART IDENTITY CASCADE;"))
        
        session.commit()
        
        logger.info("------------------------------------------------")
        logger.info(f"✅ ÉXITO: Se eliminaron {num_borrados} registros.")
        logger.info("   La base de datos está ahora VACÍA (0 usuarios).")
        logger.info("------------------------------------------------")

    except Exception as e:
        session.rollback()
        logger.error(f"❌ ERROR AL BORRAR: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    borrar_todo_absolutamente()
