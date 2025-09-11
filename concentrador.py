# -*- coding: utf-8 -*-

# Import package
import paho.mqtt.client as mqtt # type: ignore
import ssl
import time
from time import ctime
import math
from collections import deque
from datetime import datetime, timedelta

import threading
import queue
import boto3 # type: ignore
import struct
from decimal import Decimal

import json
import sys  # <--- Para poder cerrar el script si hay un error

# CONFIGURACION PDC
# ID_PDC = 50
# WAIT_TIME = 1  # maximo tiempo que espero un dato, en segundos
# LISTA_PMUS = [1, 2]
# NUM_FUENTES = int(len(LISTA_PMUS))

# --- Variables Globales ---
db_queue = queue.Queue()
config_lock = threading.Lock() # Candado global de acceso a config

# table for calculating CRC
CRC16_XMODEM_TABLE = [
    0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
    0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
    0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
    0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
    0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
    0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
    0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
    0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
    0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
    0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
    0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
    0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
    0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41,
    0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
    0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
    0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
    0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
    0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
    0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
    0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
    0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
    0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
    0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
    0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
    0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
    0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
    0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
    0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
    0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
    0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
    0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
    0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0,
]

# Define Variables
# MQTT_PORT = 8883
MQTT_KEEPALIVE_INTERVAL = 45
# MQTT_TOPIC = 'nuevo_topico'
# MQTT_MSG = '{"message": "Hola Pa, soy Pablo desde IoT Amazon"}'

# MQTT_HOST = 'a1ddn6piu1sj2t-ats.iot.us-west-2.amazonaws.com'
# CA_ROOT_CERT_FILE = 'AmazonRootCA1.pem'
# THING_CERT_FILE = 'concentrador_aws-certificate.pem.crt'
# THING_PRIVATE_KEY = 'concentrador_aws-private.pem.key'

# array con la lista de IDs de donde se recibe informacion
lista_ids = []
# diccionario de configs, cada una tiene un key que es el ID que le corresponde
configs = {}
# diccionario de longitud de datos, cada una tiene un key que es el ID que le corresponde
longitud_datos = {}
# diccionario de datos, cada una tiene un key que es el tiempo que le corresponde
datos = {}
# lista de tiempos en espera, para hacer push y pop de datos en concentrador
tiempos = deque([])

# elementos para conformar config
timebase_bin = [0xFF, 0xFF, 0xFF]
timebase = 16777215

def parsear_trama_de_datos_pdc(frame_data):
    """
    Decodifica una trama de datos binaria consolidada y devuelve un diccionario 
    con los valores convertidos a tipo Decimal para DynamoDB.
    """
    decoded_dict = {}
    offset = 14
    payload = frame_data[offset:-2]

    data_offset = 0
    # Lectura Protegida de config y lista_ids
    with config_lock:
        # Se leen las variables compartidas (lista_ids y configs) dentro del candado
        # para asegurar que no cambien a mitad de la operación.
        for pmu_id in sorted(lista_ids):
            pmu_data = {}
            pmu_config_chunk = configs.get(pmu_id)
            if not pmu_config_chunk:
                continue

            _STN_LEN = 16
            _IDCODE_SRC_LEN = 2
            _FORMAT_WORD_OFFSET = _STN_LEN + _IDCODE_SRC_LEN
            _PHNMR_OFFSET = _FORMAT_WORD_OFFSET + 2

            formato_byte = pmu_config_chunk[_FORMAT_WORD_OFFSET + 1]
            es_rectangular = True if (formato_byte & 0x02) >> 1 else False
            num_fasores = pmu_config_chunk[_PHNMR_OFFSET] << 8 | pmu_config_chunk[_PHNMR_OFFSET + 1]

            data_offset += 2  # STAT

            phasors = []
            for _ in range(num_fasores):
                values = struct.unpack('!ff', payload[data_offset:data_offset+8])
                if es_rectangular:
                    # Convertimos a Decimal usando un string intermedio para no perder precisión
                    phasors.append({
                        'real': Decimal(str(values[0])), 
                        'imag': Decimal(str(values[1]))
                    })
                else:
                    phasors.append({
                        'mag': Decimal(str(values[0])), 
                        'ang': Decimal(str(values[1]))
                    })
                data_offset += 8
            pmu_data['phasors'] = phasors

            freq, rocof = struct.unpack('!ff', payload[data_offset:data_offset+8])
            # Convertimos también frecuencia y ROCOF a Decimal
            pmu_data['frequency'] = Decimal(str(freq))
            pmu_data['rocof'] = Decimal(str(rocof))
            data_offset += 8
            
            decoded_dict[str(pmu_id)] = pmu_data
        
    return decoded_dict

def guardar_en_dynamodb_worker(table_name, region, ttl_days):
    """
    Este hilo toma datos CRUDOS de la cola, los DECODIFICA, añade el
    atributo TTL y los guarda en DynamoDB.
    """
    try:
        dynamodb = boto3.resource('dynamodb', region_name=region)
        table = dynamodb.Table(table_name)
    except Exception as e:
        print(f"ERROR HILO DB: No se pudo conectar a DynamoDB. El hilo terminará. Error: {e}")
        return

    print(f"INFO HILO DB: Hilo de DynamoDB iniciado. Los datos expirarán en {ttl_days} días.")

    while True:
        try:
            item_crudo = db_queue.get()
            
            datos_decodificados = parsear_trama_de_datos_pdc(item_crudo['RawData'])
            
            # --- CÁLCULO DEL TIMESTAMP DE EXPIRACIÓN (TTL) ---
            timestamp_actual = item_crudo['Timestamp']
            vida_util = timedelta(days=ttl_days)
            fecha_actual = datetime.fromtimestamp(timestamp_actual)
            fecha_expiracion = fecha_actual + vida_util
            timestamp_ttl = int(fecha_expiracion.timestamp())
            # --- FIN DEL CÁLCULO ---
            
            item_final = {
                'PDC_ID': item_crudo['PDC_ID'],
                'Timestamp': Decimal(str(item_crudo['Timestamp'])),
                'TimestampUTC': item_crudo['TimestampUTC'],
                'PMUsReporting': item_crudo['PMUsReporting'],
                'DecodedData': datos_decodificados,
                'ttl': timestamp_ttl
            }
            
            table.put_item(Item=item_final)
            db_queue.task_done()

        except Exception as e:
            print(f"ERROR HILO DB: Fallo al procesar o guardar en DynamoDB: {e}")



def decodificar_datos(data):
    global timebase
    global lista_ids
    global datos
    global longitud_datos
    global tiempos

    idcode = data[4] << 8 | data[5]
    try:
        # si no encuentra el idcode en la lista, va a fallar la ejecucion,
        # asi que funciona como un if de si encuentra el index o no
        idx = lista_ids.index(idcode)
    except ValueError:
        return  # si el id no esta en la lista, entonces no tengo informacion como para agregar los datos, asi que los descarto

    # obtengo el soc y fracsec para definir si ya hay datos de ese tiempo o no
    soc = data[6] << 24 | data[7] << 16 | data[8] << 8 | data[9]
    fracsec = data[10] << 24 | data[11] << 16 | data[12] << 8 | data[13]
    tiempo = soc + fracsec / timebase

    # obtengo cantidad de datos utiles
    largo_datos_utiles = (data[2] << 8 | data[3])

    if tiempo in datos:  # ese tiempo en datos ya existe
        datos[tiempo][idx] = data[14:largo_datos_utiles - 2] # El largo incluye el CRC, por eso -2 en vez de -16
    else:
        datos[tiempo] = [None] * NUM_FUENTES
        datos[tiempo][idx] = data[14:largo_datos_utiles - 2]
        tiempos.append(tiempo)  # agrego al buffer FIFO el tiempo incorporado

    if len(tiempos) > WAIT_TIME * 50:
        emitir_datos()

def decodificar_config(data):
    global timebase_bin
    global timebase
    global lista_ids
    global configs
    global longitud_datos
    global LISTA_PMUS

    idcode = data[4] << 8 | data[5]
    print(idcode)
    if idcode not in LISTA_PMUS:
        return # si el id no esta en la lista, entonces no quiero datos de esa PMU

    # Escritura Protegida de config y lista_ids
    with config_lock:
        if idcode not in lista_ids:
            lista_ids.append(idcode)  # agrego IDs a la lista

    largo_datos_utiles = (data[2] << 8 | data[3])
    configs[idcode] = data[20:(largo_datos_utiles - 4)] # El largo incluye el datarate y el CRC
    
    # Pensado para PMUS simples. No para concentradores de orden inferior
    formato = data[39]
    # formato freq
    mascara = (formato & 0x08) >> 3
    mult_freq = 4 if mascara == 1 else 2
    
    # formato analogicos
    mascara = (formato & 0x04) >> 2
    mult_analogicos = 4 if mascara == 1 else 2

    # formato fasores
    mascara = (formato & 0x02) >> 1
    mult_fasores = 8 if mascara == 1 else 4

    num_datos = (data[40] << 8 | data[41]) * mult_fasores
    num_datos += (data[42] << 8 | data[43]) * mult_analogicos
    num_datos += (data[44] << 8 | data[45]) * 2  # digitales
    num_datos += 2 * mult_freq
    num_datos += 2  # considero el stat de cada PMU
    longitud_datos[idcode] = num_datos

    # Bloque para imprimir la configuración recibida ---
    print("\n" + "="*50)
    print(f"  Configuración Recibida de PMU ID: {idcode}")
    print("="*50)

    soc = data[6] << 24 | data[7] << 16 | data[8] << 8 | data[9]
    fracsec = data[10] << 24 | data[11] << 16 | data[12] << 8 | data[13]
    frac_sec_val = fracsec & 0x00FFFFFF
    timestamp_posix = soc + (frac_sec_val / timebase)
    
    # Convertimos el timestamp POSIX a un objeto datetime y lo formateamos
    dt_object = datetime.fromtimestamp(timestamp_posix)
    timestamp_str = dt_object.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] # Formato con milisegundos

    print(f"  Timestamp de la Trama: {timestamp_str}")
    
    # Extraer valores adicionales para imprimir
    nom_estacion = data[20:36].decode('utf-8', errors='ignore').strip()
    num_fasores = data[40] << 8 | data[41]
    num_analog = data[42] << 8 | data[43]
    num_digital = data[44] << 8 | data[45]
    
    # Decodificar el byte de formato en texto para mayor claridad
    formato_freq_txt = "32-bit float" if (formato & 0x08) >> 3 else "16-bit integer"
    formato_analog_txt = "32-bit float" if (formato & 0x04) >> 2 else "16-bit integer"
    formato_fasor_txt = "Rectangular (float)" if (formato & 0x02) >> 1 else "Polar (float)"
    
    print(f"  Nombre de Estación: {nom_estacion}")
    print(f"  Formato de Frecuencia/ROCOF: {formato_freq_txt}")
    print(f"  Formato de Analógicos: {formato_analog_txt}")
    print(f"  Formato de Fasores: {formato_fasor_txt}")
    print(f"  Número de Fasores: {num_fasores}")
    print(f"  Número de Valores Analógicos: {num_analog}")
    print(f"  Número de Canales Digitales: {num_digital}")
    print("="*50 + "\n")

def _crc16(data, crc, table):
    for byte in data:
        crc = ((crc << 8) & 0xff00) ^ table[((crc >> 8) & 0xff) ^ byte]
    return crc & 0xffff

def crc_ccitt(data, crc=0xFFFF):
    return _crc16(data, crc, CRC16_XMODEM_TABLE)

def emitir_config():
    global timebase_bin
    global timebase
    global lista_ids
    global configs
    global LISTA_PMUS
    global ID_PDC

    print("Emitiendo configuración consolidada...")
    frame_config = bytearray()
    frame_config.append(0xAA)
    frame_config.append(0x31) # Tipo Trama: Configuración

    config_data = bytearray()
    # Conformo seccion de datos a enviar
    for id_pmu in sorted(lista_ids): # Ordenar para consistencia
        if id_pmu in configs:
            config_data.extend(configs[id_pmu])

    # Calculo longitud para poner de framesize
    longitud = len(config_data) + 24
    frame_config.extend(longitud.to_bytes(2, 'big'))

    # ID del PDC
    frame_config.extend(ID_PDC.to_bytes(2, 'big'))

    # Obtengo SOC y FRACSEC
    tiempo_actual = time.time()
    soc = int(tiempo_actual)
    fracsec_float = (tiempo_actual - soc) * timebase
    fracsec = int(fracsec_float) & 0x00FFFFFF

    frame_config.extend(soc.to_bytes(4, 'big'))
    frame_config.extend(fracsec.to_bytes(4, 'big'))

    # TIMEBASE
    frame_config.append(0x00)
    frame_config.extend(timebase_bin)

    # NUM PMUS
    frame_config.extend(len(lista_ids).to_bytes(2, 'big'))

    # Incorporo datos al frame
    frame_config.extend(config_data)

    # DATARATE (ej. 50 fps)
    frame_config.extend((50).to_bytes(2, 'big'))

    # CRC
    crc = crc_ccitt(frame_config)
    frame_config.extend(crc.to_bytes(2, 'big'))

    mqttc.publish(f"{ID_PDC}", frame_config, qos=1)

def emitir_datos():
    global timebase
    global lista_ids
    global longitud_datos
    global datos
    global tiempos
    global ID_PDC

    if not tiempos:
        return

    tiempo = tiempos.popleft()
    frame_data = bytearray()
    # encabezado de datos
    frame_data.append(0xAA)
    frame_data.append(0x01) # Tipo Trama: Datos

    datos_data = bytearray()
    num_pmu_reportando = 0
    # Conformo seccion de datos a enviar
    for id_pmu in sorted(lista_ids): # Ordenar para consistencia
        try:
            # Busca el índice correcto del id_pmu en la lista original de PMUs
            idx = lista_ids.index(id_pmu)
            pmu_data = datos[tiempo][idx]
            if pmu_data is not None:
                datos_data.extend(pmu_data)
                num_pmu_reportando += 1
            else:
                # Genero datos en blanco para esa PMU
                datos_data.extend(bytearray(longitud_datos[id_pmu]))
        except (ValueError, IndexError, KeyError):
            # Genero datos en blanco si algo falla
            datos_data.extend(bytearray(longitud_datos.get(id_pmu, 0)))

    # Elimino el dato del diccionario para liberar memoria
    del datos[tiempo]

    # Calculo longitud para poner de framesize
    longitud = len(datos_data) + 16
    frame_data.extend(longitud.to_bytes(2, 'big'))

    # ID del PDC
    frame_data.extend(ID_PDC.to_bytes(2, 'big'))

    # Obtengo SOC y FRACSEC del tiempo de la trama
    soc = int(tiempo)
    fracsec_float = (tiempo - soc) * timebase
    fracsec = int(fracsec_float) & 0x00FFFFFF

    frame_data.extend(soc.to_bytes(4, 'big'))
    frame_data.extend(fracsec.to_bytes(4, 'big'))

    # Incorporo datos al frame
    frame_data.extend(datos_data)

    # CRC
    crc = crc_ccitt(frame_data)
    frame_data.extend(crc.to_bytes(2, 'big'))

    mqttc.publish(f"{ID_PDC}", frame_data, qos=1)

    # A la cola se envían los datos CRUDOS para que el otro hilo los procese
    item = {
        'PDC_ID': ID_PDC,
        'Timestamp': tiempo,
        'TimestampUTC': datetime.fromtimestamp(tiempo).isoformat(),
        'PMUsReporting': num_pmu_reportando,
        'RawData': frame_data
    }
    db_queue.put(item)

# --- Callbacks de MQTT ---
def on_message(client, userdata, message):
    mensaje = message.payload
    if len(mensaje) < 2: return
    
    # El identificador de tipo de trama está en el segundo byte
    identificador = (mensaje[1] & 0x70) >> 4

    if identificador == 0x03: # Trama de Configuración
        decodificar_config(mensaje)
        emitir_config()
    elif identificador == 0x00: # Trama de Datos
        decodificar_datos(mensaje)

def on_subscribe(client, userdata, mid, granted_qos, properties=None):
    print('Suscripción exitosa!')

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print('Conectado exitosamente al broker MQTT!')
        # Suscribirse a los tópicos de las PMUs
        for pmu_id in LISTA_PMUS:
            topic = f"{pmu_id}"
            print(f"Suscribiéndose al tópico: {topic}")
            client.subscribe(topic, 0)
    else:
        print(f"Fallo en la conexión, código de retorno: {rc}")

def on_publish(client, userdata, mid, rc, properties=None):
    # Descomentar si se necesita depurar cada publicación
    # print(f"Publicado mensaje MID: {mid}")
    pass

# --- Inicio del Script ---

# --- Carga la configuracion desde JSON ---
def cargar_configuracion(archivo):
    """Carga un archivo de configuración JSON y maneja errores."""
    try:
        with open(archivo, 'r') as f:
            config = json.load(f)
        print(f"Configuración cargada desde '{archivo}'")
        return config
    except FileNotFoundError:
        print(f"Error: El archivo de configuración '{archivo}' no se encontró.")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"Error: El archivo de configuración '{archivo}' no es un JSON válido.")
        sys.exit(1)
    except Exception as e:
        print(f"Error inesperado al leer '{archivo}': {e}")
        sys.exit(1)


config_nube = cargar_configuracion('config_nube.json')
config_pdc = cargar_configuracion('config_pdc.json')
try:
   # Parámetros de conexión
    MQTT_HOST = config_nube['MQTT_HOST']
    MQTT_PORT = config_nube['MQTT_PORT']
    CA_ROOT_CERT_FILE = config_nube['CA_ROOT_CERT_FILE']
    THING_CERT_FILE = config_nube['THING_CERT_FILE']
    THING_PRIVATE_KEY = config_nube['THING_PRIVATE_KEY']
    AWS_REGION = config_nube['AWS_REGION']
    DYNAMODB_TABLE_NAME = config_nube['DYNAMODB_TABLE_NAME']
    
    # Parámetros del PDC
    ID_PDC = config_pdc['ID_PDC']
    WAIT_TIME = config_pdc['WAIT_TIME']
    LISTA_PMUS = config_pdc['LISTA_PMUS']
    TTL_DAYS = config_pdc['TTL_DAYS']
    NUM_FUENTES = int(len(LISTA_PMUS))
    
    print(f"Claves JSON cargadas")

except KeyError as e:
    print(f"Error: La clave {e} falta en uno de los archivos de configuración.")
    sys.exit(1)
# -----------------------------------------------------------------
# --- Crear e iniciar el hilo para la base de datos ---
db_thread = threading.Thread(
    target=guardar_en_dynamodb_worker,
    args=(DYNAMODB_TABLE_NAME, AWS_REGION, TTL_DAYS),
    daemon=True
)
db_thread.start()

# Initiate MQTT Client
mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2) # Compatible con la mayoria de brokers
mqttc.on_subscribe = on_subscribe
mqttc.on_connect = on_connect
mqttc.on_publish = on_publish
mqttc.on_message = on_message
print('Cliente MQTT creado.')

# Configure TLS Set
try:
    mqttc.tls_set(CA_ROOT_CERT_FILE, certfile=THING_CERT_FILE, keyfile=THING_PRIVATE_KEY,
                  cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLSv1_2, ciphers=None)
    print('Certificados TLS configurados.')
    # Connect with MQTT Broker
    mqttc.connect(MQTT_HOST, MQTT_PORT, MQTT_KEEPALIVE_INTERVAL)
    mqttc.loop_start()

    # Mantener el script corriendo
    while True:
        time.sleep(1)

except FileNotFoundError:
    print("Error: No se encontraron los archivos de certificado. Asegúrate de que las rutas son correctas.")
except Exception as e:
    print(f"Ocurrió un error: {e}")