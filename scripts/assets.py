import os
import urllib.request as req

base_url = "https://docs.google.com/spreadsheets/d/e/"
sheet = "2PACX-1vRSB3_JCKkvYQkgEwYW0PkzMJDovwvMwX28B5ainGuDirimi6n4n1nryc0Pbb0fHCfsZVYAnqobgP8D"

images = [
    "574263984", "1320291746",
    "1939554456", "141578891",
    "744871918", "2074125212",
    "1201195179", "1744392307",
    "857919432", "425905901",
    "851362461", "395420450",
    "1329486679", "1492441660",
    "827148403", "22096209",
    "1848022003", "873256307",
    "2077796175", "1058712731",
    "1978363820", "1357338484",
    "1038688506", "591742088",
    "603335823", "1958520312",
    "1683681566", "373318070",
    "2063902375", "1924052371",
    "1821951025", "1074834619",
    "1322547223", "682972572",
    "731574492", "119707745",
    "842189614", "1158305404",
    "1047100711", "1781225090",
    "132326038", "259061157",
    "122662822", "121562673",
    "987529461", "1299383115",
    "454080210", "1279668502",
    "105471492", "1092961723",
    "2095693594", "134138183"
]

for img in images:
    oid = img
    print(f"Object id:{oid}")
    img_url = f"{base_url}{sheet}/pubchart?oid={oid}&format=image"
    image_file = os.path.abspath(f"../web/charts/{oid}.png")
    if os.path.exists(image_file):
        os.remove(image_file)
    req.urlretrieve(img_url, image_file)
