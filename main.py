########################################################################################
"""
  Universidad del Valle de Guatemala
  Departamento de Ciencias de la Computación
  Bases de datos 2 - Sección 20  
"""

__author__ = ["Cristian Laynez", "Elean Rivas", "Javier Alvarez"]
__status__ = "Students of Computer Science"

"""
    Proyecto 2: Simulador de HBase
    
    Main:
"""
########################################################################################

from hbase import * # Importar clases para controlar las tablas

# Importar otro tipo de librerias
import os

# Contenido a ejecutar
def main():
    
    hbase_database = HBaseDatabase() # Iniciar clase hbase donde se guardaran las tablas

    # Cargar set de datos inicial
    hbase_table = HBaseTable("employees", ["personal_data", "professional_data"])

    hbase_database.add_table(hbase_table) # Agregar tabla a la database

    print(hbase_database.get_tables()) # Mostrar tablas

    # Actualizar tabla
    print(hbase_database.put_data_on_table("employees", "Geoffrey", "personal_data:age", 32))

    print(hbase_database.get_tables()) # Mostrar tablas

    hbase_commands = [
        "create", "describe", "alter", "put", "scan", "get", 
        "list", "disable", "is_enabled", "drop", "drop_all",
        "delete", "delete_all", "count", "truncate"
    ]
    
    finish = False
    comand_count = 1
    while not finish:        
        terminal_consult = input(f"hbase(main):{comand_count}:0> ")

        # ? =============================================================================
        # ? ==> Verificar comandos ingresados

        # ! Detectar si el string esta vacio
        if(terminal_consult.isspace()):
            print("\nThe inserted command is empty\n")
            continue

        # ! Limpiar la pantalla 
        if(terminal_consult == "clear" or terminal_consult == "cls"):
            os.system('clear')
            continue

        # ! Salir del programa
        if(terminal_consult == "exit"):
            finish = True
            continue

        # Obtener por medio de una lista todos los argumentos solicitados
        terminal_consult = terminal_consult.split(" ")

        # ! Vamos a detectar si uno de los comandos es existente
        if(terminal_consult[0] in hbase_commands):
            result_consult = hbase_command(terminal_consult)
            print(result_consult)

        # ! Examinar si una de las palabras es valida
        else:
            print(f"\nInvalid comand\n")

        # ? =============================================================================
        
        comand_count += 1 # Vamos a contar todos los comandos escritos

# Comando de hbase a ejecutar
def hbase_command(consult : str) -> str:
    print(consult)
    
    return "" # Si todo esta en orden

# Ejecutar programa principal
if __name__ == "__main__":
    main()