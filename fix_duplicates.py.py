import logging
from sqlalchemy import func
from database import SessionLocal, engine
from models import GameCoinUser, Base

# Configuración de Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FixDuplicates")

def fusionar_duplicados():
    session = SessionLocal()
    try:
        logger.info("🔍 Analizando base de datos en busca de duplicados...")
        
        # 1. Encontrar emails duplicados
        # Hacemos una consulta agrupando por email y contando cuántas veces aparece
        duplicados_query = (
            session.query(GameCoinUser.email)
            .group_by(GameCoinUser.email)
            .having(func.count(GameCoinUser.email) > 1)
            .all()
        )
        
        if not duplicados_query:
            logger.info("✅ No se encontraron duplicados. La base de datos está limpia.")
            return

        logger.info(f"⚠️ Se encontraron {len(duplicados_query)} correos con registros duplicados.")

        # 2. Procesar cada caso de duplicidad
        count_fusionados = 0
        for (email,) in duplicados_query:
            # Traemos TODOS los registros de ese email
            registros = session.query(GameCoinUser).filter_by(email=email).all()
            
            # Estrategia de Selección del "Maestro" (El mejor registro)
            # Priorizamos:
            # 1. El que tenga RUT (suele ser el más oficial)
            # 2. El que tenga Nombre (no vacío)
            # 3. El que tenga mayor ID (el más reciente)
            
            # Ordenamos la lista según calidad de datos
            registros.sort(
                key=lambda u: (
                    u.rut is not None and u.rut != "",  # Tiene RUT? (True > False)
                    u.name is not None and u.name != "", # Tiene Nombre?
                    u.id # ID más alto
                ), 
                reverse=True # Los mejores quedan al principio (índice 0)
            )
            
            maestro = registros[0]
            sobrantes = registros[1:]
            
            saldo_acumulado = 0
            ids_eliminados = []
            
            # 3. Fusionar Saldos y Eliminar Sobrantes
            for s in sobrantes:
                saldo_acumulado += s.saldo
                ids_eliminados.append(str(s.id))
                session.delete(s) # Marcamos para borrar
            
            # Actualizamos al maestro
            saldo_antiguo = maestro.saldo
            maestro.saldo += saldo_acumulado
            
            logger.info(f"✨ Fusionando {email}:")
            logger.info(f"   - Maestro ID: {maestro.id} (Nombre: {maestro.name})")
            logger.info(f"   - Eliminados IDs: {', '.join(ids_eliminados)}")
            logger.info(f"   - Saldo fusionado: {saldo_antiguo} + {saldo_acumulado} = {maestro.saldo}")
            
            count_fusionados += 1

        # 4. Confirmar Cambios
        session.commit()
        logger.info(f"🚀 ¡Éxito! Se unificaron {count_fusionados} usuarios duplicados.")

    except Exception as e:
        session.rollback()
        logger.error(f"❌ Error crítico durante la fusión: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    print("--- INICIANDO SCRIPT DE LIMPIEZA ---")
    fusionar_duplicados()
    print("--- FIN DEL PROCESO ---")