CREATE VIEW m33_schem (age_mil, wavelength, flam, is_peculiar)
  AS
  SELECT 
    cast(cleaned_data.age AS BIGINT), 
    cast(cleaned_data.data[0] AS DOUBLE), 
    cast(cleaned_data.data[1] AS DOUBLE),
    cleaned_data.is_peculiar
  FROM (
    SELECT 
      regexp_extract(INPUT__FILE__NAME, '(hmix\\.a)(\\d*)', 2) AS age, 
      split(trim(row_str), '  ') AS data,
      field(peculiarity, 'nocp', 'cp') - 1 AS is_peculiar
    FROM m33_raw
  ) cleaned_data;