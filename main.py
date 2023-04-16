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

hbase_database = HBaseDatabase() # Iniciar clase hbase donde se guardaran las tablas

# Contenido a ejecutar
def main():    
    # --> Cargar set de datos inicial
    initial_set()

    # Definir todos los comandos existentes
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
    print(consult) # ! Para fines de "DEBUG"
    command = consult[0]

    # ==> Para crear una nueva tabla
    if(command == 'create'):
        # Verificar si cumple con el minimo de columnas (que seria como minimo 1)    
        if (len(consult) <= 2): 
            return "ERROR: You dont have define minimun a column for the new table"
        
        # Verificar que no tenga el mismo nombre de otras tablas existentes
        if(hbase_database.find_duplicate_table(consult[1])):
            return f"ERROR: Table {consult[1]} is done allready exist"
        
        # Si todo esta en orden entonces se creara la nueva tabla
        new_hbase_table = HBaseTable(consult[1], consult[2:])
        hbase_database.add_table(new_hbase_table)
        return f"=> HBase::Table - {consult[1]}" 

    # ==> Para actualizar y/o agregar datos una tabla existente


    # ==> Consultar datos de la tabla
    if(command == 'scan'):
        
        return f" row(s) in 0.0 seconds"
    
    return ""

def initial_set():
    hbase_table = HBaseTable("employees", ["personal_data", "professional_data"])

    hbase_database.add_table(hbase_table) # Agregar tabla a la database

    # --> Actualizar tabla de set de datos inicial
    # ! Vamos a implementar a Geoffrey
    hbase_database.put_data_on_table("employees", "Geoffrey", "personal_data:age", 32)
    hbase_database.put_data_on_table("employees", "Geoffrey", "professional_data:department", "sales")
    hbase_database.put_data_on_table("employees", "Geoffrey", "professional_data:salary", 42000)
    # ! Vamos a implementar a Petter
    hbase_database.put_data_on_table("employees", "Petter", "personal_data:age", 43)
    hbase_database.put_data_on_table("employees", "Petter", "personal_data:pet", "dog")
    hbase_database.put_data_on_table("employees", "Petter", "professional_data:department", "sales")
    hbase_database.put_data_on_table("employees", "Petter", "professional_data:salary", 34000)
    # ! Vamos a implementar a Joseph
    hbase_database.put_data_on_table("employees", "Joseph", "personal_data:age", 24)
    hbase_database.put_data_on_table("employees", "Joseph", "professional_data:department", "dev")
    hbase_database.put_data_on_table("employees", "Joseph", "professional_data:salary", 54000)
    # ! Vamos a implementar a Sara
    hbase_database.put_data_on_table("employees", "Sara", "personal_data:age", 29)
    hbase_database.put_data_on_table("employees", "Sara", "personal_data:pet", "cat")
    hbase_database.put_data_on_table("employees", "Sara", "professional_data:department", "hr")
    hbase_database.put_data_on_table("employees", "Sara", "professional_data:salary", 49000)

# Ejecutar programa principal
if __name__ == "__main__":
    main()