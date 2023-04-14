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

import os

# Contenido a ejecutar
def main():
    finish = False
    comand_count = 1
    while not finish:        
        terminal_consult = input(f"hbase(main):{comand_count}:0> ")

        # Vamos a verificar si el comando solicitado existe
        if(terminal_consult == "clear" or terminal_consult == "cls"):
            os.system('clear')
        elif(terminal_consult == "exit"):
            finish = True
        else:
            print(f"\nInvalid comand\n")
        
        comand_count += 1 # Vamos a contar todos los comandos escritos

# Ejecutar programa principal
if __name__ == "__main__":
    main()