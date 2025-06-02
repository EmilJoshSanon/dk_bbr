# Packages

from fastapi.testclient import TestClient

# Modules

from src.api_main import app
from src.env import API_KEY

client = TestClient(app)


# %%
def test_get_valid_current_bygning():
    query = {
        "id_lokal_id": "918d292d-eb04-4e5d-b9d0-d8026e9e0bd6",
        "gyldig": True,
        "ibrug": True,
    }
    response = client.post("/bygning", json=query, headers={"token": API_KEY})
    assert response.status_code == 200
    assert response.json() == [
        {
            "id": "51226bc9-7a0c-6b9a-7184-049053c95a2d",
            "byg007_bygningsnummer": 2,
            "byg021_bygningens_anvendelse": 930,
            "id_lokal_id": "918d292d-eb04-4e5d-b9d0-d8026e9e0bd6",
            "grund": "5e0ae4a8-b4b3-479d-ab28-864a9bcbc753",
            "virkning_fra": "2025-05-20T06:01:27.961349Z",
            "registrering_fra": "2025-05-20T06:01:27.961349Z",
        }
    ]


# %%
def test_get_invalid_token():
    query = {
        "id_lokal_id": "918d292d-eb04-4e5d-b9d0-d8026e9e0bd6",
        "gyldig": True,
        "ibrug": True,
    }
    response = client.post("/bygning", json=query, headers={"token": "invalid"})
    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid token"}
