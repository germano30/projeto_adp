CREATE TABLE BridgeFactMinimumWageFootnote(
    WageID INT, 
    FootnoteID INT,
    Context TEXT,
    CONSTRAINT PK_BridgeFactMinimumWageFootnote PRIMARY KEY (WageID, FootnoteID),
    CONSTRAINT FK_FactMinimumWage FOREIGN KEY (WageID) REFERENCES FactMinimumWage (ID),
    CONSTRAINT FK_DimFootnote FOREIGN KEY (FootnoteID) REFERENCES DimFootnote (ID)
);