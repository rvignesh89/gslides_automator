# Google Slides Automator 📠

This repository contains a python package for generating Google Slide reports for different combinations of entities for which the data is available. On a high level it works by replacing placeholders in a slide template with data from a Google Sheet.

Following elements in a slide can be replaced,
1. Text placeholders in a paragraph
2. Charts
3. Tables
4. Pictures

The real value of this package is the ability to generate a report for different entities. By providing the raw data for each entity the package in a structured format the code can generate reports for each entity automatically. This is useful when you have a large number of entities and you want to generate reports of the same structure for each entity automatically.

## Getting Started

Requirements to run the package are:

1. Python 3.12 or above.
2. Google Sheets, Slides and Drive API enabled service account credentials.
3. Access to Google Drive Shared Drive containing slide and data templates and raw data.

### Service Account Setup

1. Create a service account in Google Cloud Console with the following scopes:
   - https://www.googleapis.com/auth/spreadsheets
   - https://www.googleapis.com/auth/drive.readonly
   - https://www.googleapis.com/auth/drive.file
   - https://www.googleapis.com/auth/drive
   - https://www.googleapis.com/auth/presentations
2. Download the JSON key file and save it as `service-account-credentials.json` file.
3. **Important**: Give full access to the service account email (found in the JSON file as `client_email`) to the shared drive where the reports will be generated.

If you see "File not found" errors when trying to delete files, it means the service account doesn't have access to those files. The error messages will include the service account email that needs to be granted access.

## How it works

The package works exclusively on **Shared** Google Drive files only. One of the inputs to the package functions will be a Google Drive folder id. The package expects the Google Drive to have the exact structure as below.

```
/
├── L0-Raw/
│   ├── entity-1/
│   │   ├── data.csv
│   │   ├── table-performance.csv
│   │   ├── chart-profit.csv
│   │   ├── picture-distribution.png
│   │   └── ...
│   ├── entity-2/
│   │   ├── data.csv
│   │   ├── table-performance.csv
│   │   ├── chart-profit.csv
│   │   ├── picture-distribution.png
│   │   └── ...
│   └── ...
├── L1-Merged/
│   ├── entity-1/
│   │   ├── entity-1.gsheet
│   │   ├── picture-distribution.png
│   ├── entity-2/
│   │   ├── entity-2.gsheet
│   │   ├── picture-distribution.png
│   └── ...
├── L2-Slide/
│   ├── entity-1.gslide
│   ├── entity-2.gslide
│   └── ...
├── L3-PDF/
│   ├── entity-1.pdf
│   ├── entity-2.pdf
│   └── ...
├── templates/
│   ├── slide-template.gslide
│   ├── data-template.gsheet
│   └── ...
└── entities.csv
```

- **entities.csv** - A csv with three columns: first column is the entity name; the second column `Generate` controls processing and the third column `Slides` controls the slides to be generated for the entity. Rows with `Generate` set to `Y` are processed, while `N` (or blank) rows are skipped. Rows with `Slides` set to a number will generate only the slides with the given number. Numbers can be specific individually like `1,2,3` or a range like `1-3` or a combination of both like `1,2,3,5-7`.

- **data.csv** - A csv with the data for the entity. This csv will have the data for the entity and will be used to replace the placeholders in the slide template. This csv will have one row per placeholder with the value for that placeholder in column 2. An example of this csv is shown below.

```
brand_name_,Volvo
brand_age,"165 years"
profit_margin,10.5%
yearly_sales,100,123
```

The placeholders in the slide template are of the format `{{<brand_name>}}`.

- **templates/** - This folder contains 2 files. A data template gsheet file and a slide template gslide file.
``
The data template `data-template.gsheet` will have multiple sheets. Each sheet will have the data for a single element (chart/table). One sheet will have data for a element and there can be multiple elements in a spreadsheet.

The main purpose of template is to create charts to be embedded into the Google Slide report.

- **L0-Raw/**: Raw input data for each entity. This folder will have sub folders for each entity. Each entity folder will have the raw data for each slide and also the pictures used in the slides.

- **L1-Merged/**: Processed and structured data—one spreadsheet per entity, used to generate charts. This folder will have sub folders for each entity. Each entity folder will have a spreadsheet for each slide and also the pictures used in the slides. The purpose of this folder is to generate charts from the data in the spreadsheets.

- **L2-Slides/**: Google Slide reports for each entity. These are generated by copying the slide template and replacing the placeholders with the data from the L1-Data sheet. This folder will have one slide per entity.

- **L3-PDFs/**: PDF reports for each entity. These are generated by converting the Google Slide reports to PDF format.

Following is a workflow diagram to understand the flow.

![Data Flow Diagram](docs/data-flow.png)

To understand the data better refer to the drive below which contains sample data for a couple of bike dealers.

https://drive.google.com/drive/u/0/folders/1EaaTMa5H6EOuWMom_4iE6RZ51qWYf2af

### Why is L0-Raw needed?

Technically if you are able to generate data in the L1-Merged structure you do not need L0-Raw. However, not all programming languages have good API's to interact with Google Sheets like R. So to be compatible in such scenarios the library provides L0-Raw as just csv files. But if have the ability to generate merged datat for L1, skip L0 data generation.

## Quick Start

In order to use the package you need to setup the drive and the package.

### Setup Drive

### Package Usage

The library has 3 scripts to generate the data and reports.

1. l1_generate.py
2. l2_generate.py
3. l3_generate.py

The l1_generate.py script is used to generate the data for the reports. The l2_generate.py script is used to generate the slides reports. The l3_generate.py script is used to generate the PDFs from the reports.

These scripts can be used as follows:

### As a python package

The library can be used as a python package by installing it using pip.

```
pip install gslides_automator
```

Then you can use the library in your python code.

```
python -m gslides_automator.l1_generate --shared-drive-url <shared-drive-url> --service-account-credentials <service-account-credentials>
python -m gslides_automator.l2_generate --shared-drive-url <shared-drive-url> --service-account-credentials <service-account-credentials>
python -m gslides_automator.l3_generate --shared-drive-url <shared-drive-url> --service-account-credentials <service-account-credentials>
```

### As a CLI tool

The library can be used as a CLI tool by installing it using pip.

```
pip install gslides_automator
```

Then you can use the library as a CLI tool.

```
gslides_automator l1_generate --shared-drive-url <shared-drive-url> --service-account-credentials <service-account-credentials>
```

### As a package in RScript

The library can be used as a package in RScript by installing it using the following command.

```
install.packages("gslides_automator")
```

Then you can use the library in your RScript code.

```
library(reticulate)

l1_generate_via_python <- function(
  shared_drive_url,
  service_account_credentials,
  python_env = "/path/to/python-env"
) {
  reticulate::use_virtualenv(python_env, required = TRUE)

  ga <- reticulate::import("gslides_automator")

  ga$l1_generate(
    shared_drive_url = shared_drive_url,
    service_account_credentials = service_account_credentials
  )
}

result <- l1_generate_via_python(
  shared_drive_url = "https://drive.google.com/drive/folders/00000000000000",
  service_account_credentials = "/path/to/service-account-credentials.json",
)
print(result)
```

