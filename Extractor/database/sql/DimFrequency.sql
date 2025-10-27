CREATE TABLE DimFrequency(
    ID INT PRIMARY KEY,
    Description VARCHAR(20)
);

INSERT INTO DimFrequency(ID,Description)
VALUES(1, 'hourly'),
    (2,'daily'),
    (3,'weekly');