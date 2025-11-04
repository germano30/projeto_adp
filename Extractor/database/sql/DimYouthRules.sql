CREATE TABLE DimYouthRules(
    ID SERIAL PRIMARY KEY,
    StateName VARCHAR(100) NOT NULL,
    Year INT,
    CertificateType VARCHAR(100) NULL,
    RuleDescription TEXT NULL,
    IsLabor INT NULL,
    IsSchool INT NULL,
    RequirementLevel FLOAT NULL,
    AgeMin INT NULL,
    AgeMax INT NULL,
    Notes TEXT NULL,
    Footnote VARCHAR(10) NULL,
    FootnoteText TEXT NULL
)
