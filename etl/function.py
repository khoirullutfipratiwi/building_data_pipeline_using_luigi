def validation_process(data, table_name):
    print("================= Start Pipeline Validation =================")
    print("")
    
    # check data shape
    n_rows = data.shape[0]
    n_cols = data.shape[1]
    
    print(f"Pada tabel {table_name} memiliki jumlah {n_rows} baris dan {n_cols} kolom")
    print("")
    
    GET_COLS = data.columns
    
    # check data types for each column
    for col in GET_COLS:
        # calculate missing values in percentage
        get_missing_values = (data[col].isnull().sum()*100)/len(data)
        print(f"Columns {col} has percentages missing values {round(get_missing_values, 2)} %")
        
    print("")
    print("================= End Pipeline Validation =================")
    
