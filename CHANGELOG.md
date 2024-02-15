# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- This CHANGELOG file.
- Statute-level percentages to CSP state distribution data
  JSON. [#12](https://github.com/policy-design-lab/data-import/issues/12)
- Add base acres and recipient numbers to Title 1
  Commodities. [#14](https://github.com/policy-design-lab/data-import/issues/14)
- Feature to parse topline numbers and generate updated JSON
  documents. [#21](https://github.com/policy-design-lab/data-import/issues/21)
- Code to process crop insurance data and JSON files. [#19](https://github.com/policy-design-lab/data-import/issues/19)
- CRP data import program and generate JSON
  files. [#40](https://github.com/policy-design-lab/data-import/issues/40)
- Total area insured in Crop insurance JSON files. [#49](https://github.com/policy-design-lab/data-import/issues/49)
- ACEP data import program and generate JSON. [#54](https://github.com/policy-design-lab/data-import/issues/54)
- RCPP data import program and generate JSON. [#57](https://github.com/policy-design-lab/data-import/issues/57)
- Dairy and Disaster data import program and generate JSON files. [#59](https://github.com/policy-design-lab/data-import/issues/59)
- PaymentInPercentageNationwide to commodity programs. [#80](https://github.com/policy-design-lab/data-import/issues/80)

### Changed

- CSP data import program to update the category names and generate updated JSON
  files. [#4](https://github.com/policy-design-lab/data-import/issues/4)
- Title 1 Commodities code and JSON files based on new CSV
  file. [#9](https://github.com/policy-design-lab/data-import/issues/9)
- Title 1 Commodities state distribution JSON structure for bar
  chart. [#7](https://github.com/policy-design-lab/data-import/issues/7)
- SNAP data CSV and JSON files based on changes in SNAP table (May
  2023). [#23](https://github.com/policy-design-lab/data-import/issues/23)
- SNAP topline numbers based on changes in SNAP table (May
  2023). [#25](https://github.com/policy-design-lab/data-import/issues/25)
- All programs summary program to update all programs based on the topline.csv
  file. [#29](https://github.com/policy-design-lab/data-import/issues/29)
- Calculate average of base acres and recipient counts by program instead of
  totals. [#33](https://github.com/policy-design-lab/data-import/issues/33)
- Exclude 2018 base acres and recipient count data. [#35](https://github.com/policy-design-lab/data-import/issues/35)
- All programs and summary JSON files based on latest SNAP
  data. [#31](https://github.com/policy-design-lab/data-import/issues/31)
- Data import program to process Title - 1 commodities raw data in the new
  format. [#38](https://github.com/policy-design-lab/data-import/issues/38)
- Total liabilities to average liabilities in Crop Insurance
  JSON. [#47](https://github.com/policy-design-lab/data-import/issues/47)
- All programs and summary JSON files based on latest Title-1
  data. [#45](https://github.com/policy-design-lab/data-import/issues/45)
- Crop Insurance JSON files after adding the latest
  data. [#49](https://github.com/policy-design-lab/data-import/issues/49)
- Calculate payment in percentage with the state data in CRP 
  data. [#52](https://github.com/policy-design-lab/data-import/issues/52)
- DMC and SADA json updated with new title [#61](https://github.com/policy-design-lab/data-import/issues/61)
- CSP data structure updated with years information [#66](https://github.com/policy-design-lab/data-import/issues/66)
- EQIP data structure updated with years information [#67](https://github.com/policy-design-lab/data-import/issues/67)
- Title 1's subtitle D and E json updated with new structure [#64](https://github.com/policy-design-lab/data-import/issues/64)
- Output json files' state name changed to state abbreviation [#76](https://github.com/policy-design-lab/data-import/issues/76)
- Restore percentage field in subtitle D and E json files [#83](https://github.com/policy-design-lab/data-import/issues/83)
- Renaming items for title I [#84](https://github.com/policy-design-lab/data-import/issues/84)
- Renaming items for title II [#85](https://github.com/policy-design-lab/data-import/issues/85)
- Renaming Pastured cropland to Grassland in CSP json files [#92](https://github.com/policy-design-lab/data-import/issues/92)

### Fixed

- Title 1 Commodities JSON files by adding zero entries. [#6](https://github.com/policy-design-lab/data-import/issues/6)
- Title 2 CSP JSON files by adding zero entries. [#10](https://github.com/policy-design-lab/data-import/issues/10)
- Error in calculating totals in the allprograms.json. [#27](https://github.com/policy-design-lab/data-import/issues/27)
- Average calculation in Title 1 Commodities. [#36](https://github.com/policy-design-lab/data-import/issues/36)
- Average payee count parsing in Title 1 Commodities. [#43](https://github.com/policy-design-lab/data-import/issues/43)
- Added missing fields in EQIP json files [#89](https://github.com/policy-design-lab/data-import/issues/89)
- Added missing fields in CSP json files [#90](https://github.com/policy-design-lab/data-import/issues/90)