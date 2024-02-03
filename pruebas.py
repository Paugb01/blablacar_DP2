#puede que tengamos que hacer algo con el nombre del generador, especificar las coordenadas cuando son del driver( location_driver) 
#y cuando son del pasajero (passenger_location?)
'''
def split_and_verify_location(car_location, passenger_location):
    
    car_latitud, car_longitud = car_location

    passenger_latitud, passenger_longitud = passenger_location

    #logging.info("Verifying encounter...")
    
    # Verificar si las coordenadas coinciden
    encounter_verified = (car_latitud == passenger_latitud) and (car_longitud == passenger_longitud)

    return encounter_verified

car_location = (1, -3)  # Ejemplo de coordenadas del coche
passenger_location = (1, -3)  # Ejemplo de coordenadas del pasajero

if split_and_verify_location(car_location, passenger_location):
    print("mismas coordenadas, se sube al coche")

'''

#con esta funcion tenemos mas rango para que se suba el pasajero, la anterior es con el punto exacto entre coordenadas
import math

def calculate_euclidean_distance(lat1, lon1, lat2, lon2):
    return math.sqrt((lat2 - lat1)**2 + (lon2 - lon1)**2)

def split_and_verify_location(car_location, passenger_location, distance_threshold=0.1): #distancia max entre coordenadas para que se suba
    car_latitud, car_longitud = car_location
    passenger_latitud, passenger_longitud = passenger_location

    # Calcular la distancia euclidiana entre las coordenadas
    distance = calculate_euclidean_distance(car_latitud, car_longitud, passenger_latitud, passenger_longitud)

    # Verificar si la distancia es menor o igual al umbral
    encounter_verified = distance <= distance_threshold

    return encounter_verified

car_location = (1, -3)  # Ejemplo de coordenadas del coche
passenger_location = (1.0009, -3)  # Ejemplo de coordenadas del pasajero

if split_and_verify_location(car_location, passenger_location):
    print("Mismas coordenadas o cercanas, se sube al coche")




def modify_seats(message):
    # Verificar si las coordenadas coinciden utilizando la función split_and_verify_location
    if split_and_verify_location(message['car']['location'], message['passenger']['location']):
        # Obtener la cantidad actual de asientos ocupados
        occupied_seats = message.get('seats', 0)
        
        # Incrementar en 1 los asientos ocupados
        message['seats'] = occupied_seats + 1
        

    return message

# Datos de ejemplo
message = {
    'car': {'location': (1, -3)},
    'passenger': {'location': (1, -3)},
    'seats': 0
}

conclusion = modify_seats(message)
print(conclusion)



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
    
    print(f"El precio a pagar es de €{total_payment:.2f}")
    return total_payment

pay('mvp/Rutas/AvenidadeBlascoIbáñez80-Joaquin Benlloch.kml')





