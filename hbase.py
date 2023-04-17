import time

class HBaseTable:
    def __init__(self, name : str, columns : list) -> None:
        self.name = name
        self.columns = columns
        self.enabled = True
        self.data = {}

    def table_scanned(self) -> list:
        information = ["ROW\t\t\t\t\t\t\tCOLUMN+CELL"]
        amount_rows = 0
        for key, value in self.data.items():
            for column in self.columns:
                if column in value: # Si dado caso la columna existe en value entonces se podra escanear
                    for col_key, col_val in value[column].items():
                        information.append(
                            f"{key}\t\t\t\t\t\t\tcolumn={column}:{col_key}, timestamp={col_val[1]}, value={col_val[0]}"
                        )
            amount_rows += 1
        return information, amount_rows

    def table_data_row(self, row: str) -> list:
        if row in self.data:        
            information = ["COLUMN\t\t\t\t\t\t\tCELL"]
            for key, value in self.data[row].items():
                for key2, value2 in value.items():
                    information.append(
                        f"{key}:{key2}\t\t\t\t\t\t\ttimestamp={value2[1]}, value=P{value2[0]}"
                    )
            return information
        return None

    def amount_rows(self) -> int:
        return len(self.data)

    def to_string(self) -> str:
        information = f"\nName: {self.name}"
        information += f"\nColumns: {self.columns}"
        information += f"\nEnabled: {self.enabled}"
        information += f"\nData: {self.data}"
        return information

class HBaseDatabase:
    def __init__(self) -> None:
        self.tables = {}

    def add_table(self, table : HBaseTable) -> None:
        self.tables[table.name] = table

    def put_data_on_table(self, table_name : str, row_key : str, column_and_field : str, value) -> str:
        for name, table in self.tables.items():
            if name == table_name: # Accedimos dentro de la tabla
                # Vamos a verificar si la columna si es valida
                column_information = column_and_field.split(":")

                # Si la columna existe entonces podremos agregar o actualizar la informacion 
                if column_information[0] in table.columns:
                    if row_key in table.data: # Si existe entonces se acutalizara la informacion
                        # Si se creara un filed nuevo en la columna si dado caso no existe jeje
                        if column_information[0] not in table.data[row_key]:
                            table.data[row_key][column_information[0]] = {column_information[1] : [value, int(time.time())]}
                            table.data = dict(sorted(table.data.items(), reverse=False))
                            return "update field"
                        # O si no en su defecto se creara una nueva field para la columna
                        table.data[row_key][column_information[0]][column_information[1]] = [value, int(time.time())]
                        table.data = dict(sorted(table.data.items(), reverse=False))
                        return "update"
                                            
                    # Si no existe la row_key eso quiere decir que se creara
                    data = {
                        column_information[0] : {
                            column_information[1] : [value, int(time.time())]
                        },
                    }
                    table.data[row_key] = data
                    table.data = dict(sorted(table.data.items(), reverse=False))
                    return "yes"

                return "ERROR: --> Column inserted doesnt exist"
        return "ERROR: --> Table doen't exists"
    
    def find_existing_table(self, table_name: str) -> bool:
        return table_name in self.tables
    
    def scan_table(self, table_name: str):
        table = self.tables[table_name]
        return table.table_scanned()
    
    def amount_rows_table(self, table_name: str):
        return self.tables[table_name].amount_rows()
    
    def truncating_table(self, table_name: str):
        self.tables[table_name].enabled = False
        self.tables[table_name].data = {}
    
    def list_all_tables(self):
        return "\n".join(self.tables.keys()), len(self.tables.keys())

    def get_tables(self) -> str: # ! Esto estara solo para ver como se actualizan las tablas
        information = ""
        for table in self.tables.values():
            information += table.to_string();
        return information
    
    def drop_table(self, table_name: str) -> str:
        if table_name in self.tables:
            self.tables.pop(table_name)
            return True
        return "ERROR: --> La tabla no existe"
    
    def drop_all_tables(self) -> str:
        self.tables = {}
        return True
    
    def get_table(self, table_name: str, row_name: str) -> HBaseTable:        
        return self.tables[table_name].table_data_row(row_name)
    
    def get_table_not_row(self, table_name: str) -> HBaseTable:
        return f"{self.tables[table_name].to_string()}"
    
    
    def disable_table(self, table_name: str) -> str:
        if table_name in self.tables:
            self.tables[table_name].enabled = False
            return True
        return False
    
    def enable_table(self, table_name: str) -> str:
        if table_name in self.tables:
            self.tables[table_name].enabled = True
            return True
        return False
    
    def is_enabled(self, table_name: str) -> bool:
        if table_name in self.tables:
            return self.tables[table_name].enabled
            # if self.tables[table_name].enabled == True:
            #     return True
            # else:
            #     return False
        return "ERROR: --> La tabla no existe"
    