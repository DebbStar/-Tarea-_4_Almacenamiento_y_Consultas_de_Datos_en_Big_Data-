import happybase
import pandas as pd
from datetime import datetime

try:
    # 1. Establecer conexión con HBase
    connection = happybase.Connection('localhost')
    print("Conexión establecida con HBase ...\n")

    # 2. Crear la tabla con las familias de columnas
    # La función dict() en Python se usa para crear diccionarios,
    # que son estructuras de datos que almacenan pares clave-valor.
    table_name = 'revenues'
    families = {
        'tomador': dict(), # información básica del tomador de la póliza
        'poliza':  dict(), # información de la póliza
        'pago':    dict(), # información de métodos de pago
        'asesor':  dict()  # información del asesor comercial
    }

    # Eliminar la tabla si ya existe
    if table_name.encode() in connection.tables():
        print(f"Eliminando tabla existente - {table_name} ...")
        connection.delete_table(table_name, disable=True)

    # Crear nueva tabla
    connection.create_table(table_name, families)
    table = connection.table(table_name)
    print(f"\nTabla {table_name} creada exitosamente ...")

    # Cargar datos del CSV
    revenue_data = pd.read_csv('data/v_revenues.csv')

    # Iterar sobre el DataFrame usando el índice
    for index, row in revenue_data.iterrows():
        # Generar row key basado en el índice
        row_key = str(row['numPoliza']).encode()

        # Organizar los datos en familias de columnas
        data = {
            # Familia: tomador
            b'tomador:nombre': str(row['nombreTomador']).encode(),
            b'tomador:documento': str(row['documentoIdentidad']).encode(),

            # Familia: poliza
            b'poliza:fecha_vigencia': datetime.strptime(row['fechaVigencia'], "%d/%m/%Y").strftime("%Y-%m-%d").encode(),
            b'poliza:ramo': str(row['ramoPoliza']).encode(),
            b'poliza:tipo': str(row['tipoPoliza']).encode(),
            b'poliza:monto': str(row['monto']).encode(),

            # Familia: pago
            b'pago:metodo': str(row['metodoPago']).encode(),
            #b'pago:pin': str(row['pinTransaccion']).encode(),

            # Familia: asesor
            b'asesor:nombre': str(row['nombreAsesorComercial']).encode()
        }

        table.put(row_key, data)

        print("Datos cargados exitosamente ...")

        # 4. Consultas y Análisis de Datos
        print("\n=== Todas las pólizas en la base de datos (primeros 3) ===")
        count = 0
        for key, data in table.scan():
            if count < 3:  # Limitamos a 3 para el ejemplo
                print(f"Tomador:   {data[b'tomador:nombre'].decode()}")
                print(f"Documento: {data[b'tomador:documento'].decode()}")
                print(f"Pago:      {data[b'pago:metodo'].decode()}")
                print(f"Monto:     {data[b'poliza:monto'].decode()}\n")
                count += 1

        # 5. Encontrar pólizas por rango de precio
        print("\n=== Pólizas con precio menor a $250,000 ===")
        for key, data in table.scan():
            if int(data[b'poliza:monto'].decode()) < 250000:
                print(f"Tomador: {data[b'tomador:nombre'].decode()}")
                print(f"Tipo:    {data[b'poliza:tipo'].decode()}")
                print(f"Monto:   {data[b'poliza:monto'].decode()}")

        # 6. Análisis de pólizas
        print("\n=== Pólizas por métodos de pago ===")
        policy_stats = {}
        for key, data in table.scan():
            payment = data[b'pago:metodo'].decode()
            policy_stats[payment] = policy_stats.get(payment, 0) + 1

        print("Iniciando conteo ...")
        for payment, count in policy_stats.items():
            print(f"{payment}: {count}")

        # 7. Análisis de precios por método de pago
        print("\n=== Precio promedio por método de pago ===")
        policy_prices = {}
        policy_counts = {}

        for key, data in table.scan():
            payment = data[b'pago:metodo'].decode()
            price = int(data[b'poliza:monto'].decode())

            policy_prices[payment] = policy_prices.get(payment, 0) + price
            policy_counts[payment] = policy_counts.get(payment, 0) + 1

        for payment in policy_prices:
            avg_price = policy_prices[payment] / policy_counts[payment]
            print(f"{payment}: {avg_price:.2f}")


except Exception as e:
    print(f"Error: {str(e)}")
finally:
    # Cerrar la conexión
    connection.close()
