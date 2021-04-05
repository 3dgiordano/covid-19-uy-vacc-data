import os
import urllib.request as req

base_url = "https://docs.google.com/spreadsheets/d/e/"
sheet = "2PACX-1vRSB3_JCKkvYQkgEwYW0PkzMJDovwvMwX28B5ainGuDirimi6n4n1nryc0Pbb0fHCfsZVYAnqobgP8D"

images = [
    "574263984",
    "1939554456",
    "744871918",
    "1201195179",
    "857919432", "425905901",
    "851362461",
    "1329486679", "1492441660",
    "827148403", "22096209",
    "1848022003", "873256307",
    "2077796175", "1058712731",
    "1978363820",
    "1038688506",
    "603335823",
    "1683681566",
    "2063902375",
    "1821951025",
    "1322547223",
    "731574492",
    "842189614",
    "1047100711",
    "132326038",
    "122662822",
    "987529461", "1299383115",
    "454080210",
    "105471492",
    "2095693594",
]

for img in images:
    oid = img
    print(f"Object id:{oid}")
    img_url = f"{base_url}{sheet}/pubchart?oid={oid}&format=image"
    image_file = os.path.abspath(f"../web/charts/{oid}.png")
    if os.path.exists(image_file):
        os.remove(image_file)
    req.urlretrieve(img_url, image_file)
