from scipy.spatial import distance


# Aproximación euclidea a kms. Si actual_dist entre coche y pasajero < 0,6km --> match
def verify_match(car_location, passenger_location):

    actual_dist = distance.euclidean(car_location, passenger_location) * 111.319
    if (actual_dist<=0.1):
            match = True
    else:
        match = False
    print(actual_dist)        

    return match 


def modify_seats(message):
    # Verificar si las coordenadas coinciden utilizando la función split_and_verify_location
    if verify_match(message['car']['location'], message['passenger']['location']):
        # Obtener la cantidad actual de asientos ocupados
        occupied_seats = message.get('seats', 0)
        
        # Incrementar en 1 los asientos ocupados
        message['seats'] = occupied_seats + 1
        print(f'se sube al coche')
    else:
        print(f'el coche está muy lejos, dame otras coordenadas')

    return message


def pay(file_path):
    total_payment = 0.0

    with open(file_path, 'r') as file:
        for linea in file:
            # Verificar si la línea contiene coordenadas y no contiene caracteres no deseados
            if ',' in linea and 'name' not in linea:
                # Dividir la línea en partes usando la coma como delimitador
                parts = linea.strip().split(',')
                
                # Verificar si hay al menos dos partes (latitud y longitud)
                if len(parts) >= 2:
                    # Tomar las primeras dos partes y convertirlas a float
                    latitud, longitud = map(float, parts[:2])
                    # Realizar el pago (10 centavos por coordenada)
                    total_payment += 0.10
    return total_payment
