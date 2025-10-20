CREATE TABLE BridgeFactMinimumWageFootnote(
    WageID INT, 
    FootnoteID INT,
    Context TEXT,
    CONSTRAINT FK_FactMinimumWage FOREIGN KEY (WageID) REFERENCES FactMinimumWage (ID),
    CONSTRAINT FK_DimFootnote FOREIGN KEY (FootnoteID) REFERENCES DimFootnote (ID)
)