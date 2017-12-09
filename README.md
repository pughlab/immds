## Source code included in **immds** repository
1. scripts for loading immune dataset into mongo db database
2. scripts to calculate the frequency of vgenes over a given population


#### Note for those running scripts
1. make sure python3 and pymongo are installed
2. checkout source code from github
3. example of running vgene frequency calculation

```
python calculate_vgene_frequency.py
    -n samwise
    -u imm_user
    -p somepassword
    -s TLML
    -o ~/Projects/tcell_clonality_blast
    -f TLML_frequency_data
```