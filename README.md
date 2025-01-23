# Updating Index's Company List
To update the company list in each index, here are the steps:
- Open idx website
- In the Menu Bar hover to "Data Pasar" and go to "Indeks Saham" in "Data Saham" sub section
- Download all the zip file for all the indices and do the checklist to make sure nothing is left out
- [ ] idxbumn20
- [ ] idxhidiv20
- [ ] idxq30
- [ ] idxg30
- [ ] economic30
- [ ] idxvesta28
- [ ] srikehati
- [ ] idxv30
- [ ] jii70
- [ ] idxesgl
- [ ] sminfra
- [ ] kompas100
- [ ] idx30
- [ ] lq45
- Move the zip file for the indices list into sectors_indices_company_list/source_data
- Run sectors_indices_company_list/data_automation.py
- Push into github with commit comment “Indices company list update {date}"
