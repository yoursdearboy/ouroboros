CREATE TABLE patients (
    id INT,
    last_name TEXT,
    first_name TEXT,
    middle_name TEXT,
    dob DATE,
    sex TEXT,
    PRIMARY KEY (id)
);

CREATE TABLE donors (
    id INT,
    last_name TEXT,
    first_name TEXT,
    middle_name TEXT,
    dob DATE,
    sex TEXT,
    PRIMARY KEY (id)
);

CREATE TABLE diagnoses (
    patient_id INT,
    icd_code TEXT,
    FOREIGN KEY(patient_id) REFERENCES patients(id)
);

CREATE TABLE donorships (
    id INT,
    patient_id INT,
    donor_id INT,
    date DATE,
    PRIMARY KEY (id),
    FOREIGN KEY(patient_id) REFERENCES patients(id),
    FOREIGN KEY(donor_id) REFERENCES donors(id)
);

INSERT INTO patients VALUES
    (1, "Ivanov", "Ivan", NULL, "1991-01-01", "M"),
    (2, "Petrov", "Petr", NULL, "1992-02-02", "M"),
    (3, "Svetova", "Svetlana", NULL, "1993-03-03", "F");

INSERT INTO diagnoses VALUES
    (1, "D61"),
    (1, "D61.9"),
    (2, "C92.0"),
    (3, "C92.0");

INSERT INTO donors VALUES
    (1, "Semenov", "Semen", NULL, "1991-06-01", "M"),
    (2, "Livov", "Lev", NULL, "1992-06-02", "M");

INSERT INTO donorships VALUES
    (1, 1, 1, "2000-01-01"),
    (2, 1, 2, "2000-02-02"),
    (3, 2, 1, "2000-03-03"),
    (4, 2, 2, "2000-04-04"),
    (5, 3, 2, "2000-05-05");
