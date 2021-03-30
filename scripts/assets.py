import urllib.request as req

base_url = "https://docs.google.com/spreadsheets/d/e/"
sheet = "2PACX-1vRSB3_JCKkvYQkgEwYW0PkzMJDovwvMwX28B5ainGuDirimi6n4n1nryc0Pbb0fHCfsZVYAnqobgP8D"

images = [
    "857919432",
    "851362461",
    "1329486679",
    "827148403",
    "1848022003",
    "2077796175",
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
    "987529461",
    "454080210",
    "105471492",
    "2095693594",
]

for img in images:
    oid = img
    img_url = f"{base_url}{sheet}/pubchart?oid={oid}&format=image"
    req.urlretrieve(img_url, f"../web/charts/{oid}.png")
