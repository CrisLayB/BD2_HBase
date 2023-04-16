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
                            f"{key}\t\t\t\t\t\t\tcolumn={column}:{col_key}, value={col_val}"
                        )
            amount_rows += 1
        return information, amount_rows

    def size_data(self) -> int:
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
                            table.data[row_key][column_information[0]] = {column_information[1] : value}
                            return "update field"
                        # O si no en su defecto se creara una nueva field para la columna
                        table.data[row_key][column_information[0]][column_information[1]] = value
                        return "update"
                                            
                    # Si no existe la row_key eso quiere decir que se creara
                    data = {
                        column_information[0] : {
                            column_information[1] : value
                        },
                    }
                    table.data[row_key] = data
                    
                    return "yes"

                return "ERROR: --> Column inserted doesnt exist"
        return "ERROR: --> Table doen't exists"
    
    def find_existing_table(self, table_name: str) -> bool:
        return table_name in self.tables
    
    def scan_table(self, table_name: str):
        table = self.tables[table_name]
        return table.table_scanned()

    def get_tables(self) -> str: # ! Esto estara solo para ver como se actualizan las tablas
        information = ""
        for table in self.tables.values():
            information += table.to_string();
        return information