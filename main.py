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
import re
import json

hbase_database = HBaseDatabase() # Iniciar clase hbase donde se guardaran las tablas

# Contenido a ejecutar
def main():    
    # --> Cargar set de datos inicial
    initial_set()

    # Definir todos los comandos existentes
    hbase_commands = [
        "create", "describe", "alter", "put", "scan", "get", 
        "list", "disable", "is_enabled", "drop", "drop_all",
        "delete", "delete_all", "count", "truncate", "enable"
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

        # ! Comando de help para entender mejor el funcionamiento del programa
        if(terminal_consult == 'help'):
            print("\nCOMMAND [create] EXAMPLE:\ncreate employees personal_data professional_data\n")
            print("COMMAND [put] EXAMPLE:\nput employees Geoffrey personal_data:pet bird\n")
            print("COMMAND [scan] EXAMPLE:\nscan employees\n")
            print("COMMAND [count] EXAMPLE:\ncount employees\n")
            print("COMMAND [truncate] EXAMPLE:\ntruncate employees\n")
            print("COMMAND [list] EXAMPLE:\nlist\n")
            print("COMMAND [get] EXAMPLE:\nget employees\n")
            print("COMMAND [get] EXAMPLE:\nget employees Geoffrey\n")
            print("COMMAND [drop] EXAMPLE:\ndrop employees\n")
            print("COMMAND [drop_all] EXAMPLE:\ndrop_all\n")
            print("COMMAND [disable] EXAMPLE:\ndisable employees\n")
            print("COMMAND [enable] EXAMPLE:\nenable employees\n")
            print("COMMAND [is_enabled] EXAMPLE:\nis_enabled employees\n")
            print("COMMAND [delete] EXAMPLE:\ndelete 'mytable', 'row1', 'cf1:col1'\n")
            print("COMMAND [delete_all] EXAMPLE:\ndelete_all 'mytable', 'row1'\n")
            print("COMMAND [describe] EXAMPLE:\ndescribe employees\n")
            print("COMMAND [alter] EXAMPLE:\nalter 'mytable', {NAME => 'new_cf'}\n")
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
        if(hbase_database.find_existing_table(consult[1])):
            return f"ERROR: Table {consult[1]} is done allready exist"
        
        # Si todo esta en orden entonces se creara la nueva tabla
        new_hbase_table = HBaseTable(consult[1], consult[2:])
        hbase_database.add_table(new_hbase_table)
        return f"=> HBase::Table - {consult[1]}" 

    # ==> Para actualizar y/o agregar datos una tabla existente
    if(command == 'put'):
        # Verificar que la consulta tenga 5 argumentos
        if len(consult) != 5:
            error_information = f"ERROR: Argument count not valid, the correct is:"
            error_information += "\nput name_table key_row column:field value"
            return error_information
        
        # Verificar si la tabla ingresada es valida
        if not hbase_database.find_existing_table(consult[1]):
            return f"ERROR: Table doesn't exist"
        # Verificar que la columna y el campo tenga ":"
        if not re.search(':', consult[3]):
            return f"ERROR: ':' was not founded"
        # Ingresar los datos deseados
        if(consult[4].isnumeric()): consult[4] = int(consult[4])
        hbase_database.put_data_on_table(consult[1], consult[2], consult[3], consult[4])
        return "0 rows(s) in 0 seconds\n"

    # ==> Consultar datos de la tabla
    if(command == 'scan'):
        # Verificar si la tabla ingresada es valida
        if not hbase_database.find_existing_table(consult[1]):
            return f"ERROR: Table doesn't exist"    
        information, rows = hbase_database.scan_table(consult[1])
        return "\n".join(information) + f"\n{rows} row(s) in 0.0 seconds"
    
    # ==> Hacer cuenta de filas en la tabla solicitada
    if(command == 'count'):
        if len(consult) <= 1: # Para determinar si faltan argumentos
            return "ERROR: Missing name of the table to count"
        
        if not hbase_database.find_existing_table(consult[1]):
            return f"ERROR: Table doesn't exist"    
        
        count = hbase_database.amount_rows_table(consult[1])
        return f"{count} row(s) in 0.0 seconds\n\n=> {count}"

    # ==> Para deshabilitar la tabla y eliminar todo su contenido
    if(command == 'truncate'):
        if len(consult) <= 1: # Para determinar si faltan argumentos
            return "ERROR: Missing name of the table to count"
        
        if not hbase_database.find_existing_table(consult[1]):
            return f"ERROR: Table doesn't exist"
        
        # Realizar las acciones para desabilitar tabla y eliminar su contenido
        hbase_database.truncating_table(consult[1])

        # Mostrar la informacion
        information = f"Truncating {consult[1]} table (it may take a while)"
        information += "\nDisabling table..."
        information += "\nTruncating table..."
        information += "\n0 row(s) in 0.0 seconds"
        return information

    # ==> Para mostrar todas las tablas presentes
    if(command == 'list'):
        tables, count = hbase_database.list_all_tables()
        return f"{tables}\n{count} row(s) in 0.0 seconds"

    if(command == 'get'):
        if len(consult) <= 1:
            return "ERROR: Not enough arguments"
        if not hbase_database.find_existing_table(consult[1]):
            return f"ERROR: Table doesn't exist"
        if len(consult) == 2:
            table = hbase_database.get_table_not_row(consult[1])
            table_str = json.dumps(table, indent=2, separators=(',', ': '))
            table_str = re.sub(r'[{}\"\']', '', table_str)
            return table_str
        elif len(consult) == 3:
            table = hbase_database.get_table(consult[1], consult[2])
            table_str = json.dumps(table, indent=2, separators=(',', ': '))
            table_str = re.sub(r'[{}\"\']', '', table_str)
            return table_str
    
    if (command == 'drop_all'):
        if len(consult) >= 2:
            return "ERROR: Too many arguments"
        drop1 = hbase_database.drop_all_tables()
        if drop1:
            return "All tables has been deleted successfully"
        else:
            return "ERROR tables couldn't be deleted"
        
    if (command == 'drop'):
        if len(consult) <= 1:
            return "ERROR: Not enough arguments"
        drop1 = hbase_database.drop_table(consult[1])
        if drop1:
            return "The table has been deleted successfully"
        else:
            return "ERROR table couldn't be deleted"
        

    if (command == 'disable'):
        if len(consult) <= 1:
            return "ERROR: Not enough arguments"
        enable = hbase_database.disable_table(consult[1])
        if enable:
            return "The table has been disabled successfully"
        else:
            return "ERROR table couldn't be disabled"
        
    
    if (command == 'enable'):
        if len(consult) <= 1:
            return "ERROR: Not enough arguments"
        enable = hbase_database.enable_table(consult[1])
        if enable:
            return "The table has been enabled successfully"
        else:
            return "ERROR table couldn't be enabled"
    # Consultar datos de la tabla
    if (command == 'describe'):
        if len(consult) <= 2:
            return "ERROR: Not enough arguments"
        describe1 = hbase_database.describe(consult[1])
        if describe1:
            return "Table descripted"
        else:
            return "ERROR table couldn't be descripted"

    if (command == 'is_enabled'):
        if len(consult) <= 1:
            return "ERROR: Not enough arguments"
        enabled = hbase_database.is_enabled(consult[1])
        if enabled:
            return "True"
        else:
            return "False"
    
    # Eliminar datos de la tabla
    if (command == 'delete'):
        if len(consult) <= 4:
            return "ERROR: Not enough arguments"
        if not hbase_database.find_existing_table(consult[1]):
            return f"ERROR: Table doesn't exist"
        delete1 = hbase_database.delete(consult[1])
        if delete1:
            return "The data has been deleted"
        else:
            return "ERROR data couln't be deleted"

    # Elimnar todos los datos de la tabla
    if (command == 'deleteall'):
        if len(consult) <= 3:
            return "ERROR: Not enough arguments"
        if not hbase_database.find_existing_table(consult[1]):
            return f"ERROR: Table doesn't exist"
        delete_all1 = hbase_database.delete_all(consult[1])
        if delete_all1:
            return "The data has been deleted"
        else:
            return "ERROR the data could not be deleted"

    # Alterar datos de la tabla
    if (command == 'alter'):
        if len(consult) <= 3:
            return "ERROR: Not enough arguments"
        if not hbase_database.find_existing_table(consult[1]):
            return f"ERROR: Table doesn't exist"
        alter1 = hbase_database.alter(consult[1])
        if alter1:
            return "The table has been modified"
        else:
            return "ERROR the table could not be modified"

    return "=> ERROR: Nothing detected"

    
        

def initial_set():
    hbase_table = HBaseTable("employees", ["personal_data", "professional_data"])

    hbase_database.add_table(hbase_table) # Agregar tabla a la database

    # --> Actualizar tabla de set de datos inicial
    # ! Vamos a implementar a Sara
    hbase_database.put_data_on_table("employees", "Sara", "personal_data:age", 29)
    hbase_database.put_data_on_table("employees", "Sara", "personal_data:pet", "cat")
    hbase_database.put_data_on_table("employees", "Sara", "professional_data:department", "hr")
    hbase_database.put_data_on_table("employees", "Sara", "professional_data:salary", 49000)
    # ! Vamos a implementar a Joseph
    hbase_database.put_data_on_table("employees", "Joseph", "personal_data:age", 24)
    hbase_database.put_data_on_table("employees", "Joseph", "professional_data:department", "dev")
    hbase_database.put_data_on_table("employees", "Joseph", "professional_data:salary", 54000)
    # ! Vamos a implementar a Petter
    hbase_database.put_data_on_table("employees", "Petter", "personal_data:age", 43)
    hbase_database.put_data_on_table("employees", "Petter", "personal_data:pet", "dog")
    hbase_database.put_data_on_table("employees", "Petter", "professional_data:department", "sales")
    hbase_database.put_data_on_table("employees", "Petter", "professional_data:salary", 34000)
    # ! Vamos a implementar a Geoffrey
    hbase_database.put_data_on_table("employees", "Geoffrey", "personal_data:age", 32)
    hbase_database.put_data_on_table("employees", "Geoffrey", "professional_data:department", "sales")
    hbase_database.put_data_on_table("employees", "Geoffrey", "professional_data:salary", 42000)

# Ejecutar programa principal
if __name__ == "__main__":    
    main()