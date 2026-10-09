"""Static reference data used to build the synthetic shop.

Brands are invented. Email domains use the reserved ".example" TLD and phone
numbers use the range the French regulator reserves for fiction, so nothing
generated here can point at a real person.
"""

from __future__ import annotations

FIRST_NAMES = [
    "Emma", "Louise", "Jade", "Alice", "Chloé", "Lina", "Léa", "Rose", "Anna",
    "Mila", "Inès", "Camille", "Sarah", "Zoé", "Manon", "Yasmine", "Nour",
    "Clara", "Julie", "Sofia", "Gabriel", "Léo", "Raphaël", "Louis", "Lucas",
    "Adam", "Arthur", "Hugo", "Jules", "Maël", "Nathan", "Théo", "Noah",
    "Mohamed", "Rayan", "Antoine", "Paul", "Maxime", "Karim", "Thomas",
]

LAST_NAMES = [
    "Martin", "Bernard", "Thomas", "Petit", "Robert", "Richard", "Durand",
    "Dubois", "Moreau", "Laurent", "Simon", "Michel", "Lefebvre", "Leroy",
    "Roux", "David", "Bertrand", "Morel", "Fournier", "Girard", "Bonnet",
    "Dupont", "Lambert", "Fontaine", "Rousseau", "Vincent", "Muller",
    "Lefèvre", "Faure", "André", "Mercier", "Blanc", "Guérin", "Boyer",
    "Garnier", "Chevalier", "François", "Legrand", "Gauthier", "Garcia",
    "Benali", "Haddad", "Nguyen", "Diallo", "Da Silva",
]

EMAIL_DOMAINS = ["courriel.example", "boitemail.example", "messagerie.example"]

# French mobile range reserved for fiction: 06 39 98 XX XX.
PHONE_PREFIX = "063998"

# (city, postal code, relative weight). Postal codes are strings on purpose:
# "06000" loses its leading zero the moment something parses it as a number.
CITIES = [
    ("Paris", "75011", 22), ("Marseille", "13006", 9), ("Lyon", "69003", 8),
    ("Toulouse", "31000", 7), ("Nice", "06000", 4), ("Nantes", "44000", 5),
    ("Montpellier", "34000", 4), ("Strasbourg", "67000", 4),
    ("Bordeaux", "33000", 5), ("Lille", "59000", 5), ("Rennes", "35000", 4),
    ("Reims", "51100", 2), ("Saint-Étienne", "42000", 2), ("Toulon", "83000", 2),
    ("Grenoble", "38000", 3), ("Dijon", "21000", 2), ("Angers", "49000", 2),
    ("Nîmes", "30000", 1.5), ("Clermont-Ferrand", "63000", 2),
    ("Le Mans", "72000", 1.5), ("Aix-en-Provence", "13100", 2),
    ("Brest", "29200", 1.5), ("Tours", "37000", 1.5), ("Amiens", "80000", 1.5),
    ("Limoges", "87000", 1), ("Annecy", "74000", 1.5), ("Perpignan", "66000", 1),
    ("Metz", "57000", 1.5), ("Besançon", "25000", 1), ("Orléans", "45000", 1.5),
    ("Rouen", "76000", 2), ("Caen", "14000", 1.5), ("Nancy", "54000", 1.5),
    ("Pau", "64000", 1), ("La Rochelle", "17000", 1), ("Ajaccio", "20000", 0.5),
    ("Bourg-en-Bresse", "01000", 0.5), ("Gap", "05000", 0.3),
]

# category -> {"brands": [...], "types": [(product type, min price, max price)]}
CATALOG = {
    "Électronique": {
        "brands": ["Novalis", "Kerwan", "Oryx Audio", "Lumio"],
        "types": [
            ("Casque audio", 35, 180), ("Enceinte Bluetooth", 25, 150),
            ("Clavier mécanique", 45, 140), ("Souris sans fil", 15, 70),
            ("Chargeur USB-C", 12, 45), ("Montre connectée", 60, 280),
            ("Webcam HD", 30, 110), ("Batterie externe", 18, 65),
        ],
    },
    "Maison & Cuisine": {
        "brands": ["Maison Altéa", "Brindille", "Cuivre & Co", "Atelier Mira"],
        "types": [
            ("Bouilloire", 25, 80), ("Poêle antiadhésive", 20, 75),
            ("Lampe de bureau", 22, 95), ("Set de couteaux", 35, 160),
            ("Aspirateur balai", 90, 320), ("Cafetière à piston", 18, 55),
            ("Plaid", 20, 70), ("Diffuseur d'huiles essentielles", 22, 60),
        ],
    },
    "Mode": {
        "brands": ["Bleu Garance", "Tilleul", "Atelier Nord", "Sillage"],
        "types": [
            ("T-shirt coton bio", 15, 40), ("Sweat à capuche", 35, 85),
            ("Jean droit", 45, 120), ("Baskets", 55, 150),
            ("Écharpe en laine", 25, 70), ("Sac à dos", 35, 110),
            ("Ceinture en cuir", 25, 65), ("Bonnet", 12, 35),
        ],
    },
    "Sport & Loisirs": {
        "brands": ["Altitude 31", "Vento", "Kairn", "Foulée"],
        "types": [
            ("Tapis de yoga", 18, 65), ("Gourde isotherme", 15, 40),
            ("Haltères 5 kg", 20, 55), ("Corde à sauter", 8, 25),
            ("Sac de sport", 25, 70), ("Lampe frontale", 18, 60),
            ("Ballon de football", 15, 45), ("Chaussettes de running", 9, 22),
        ],
    },
    "Beauté": {
        "brands": ["Éclat Végétal", "Rosée", "Maison Lys", "Onde"],
        "types": [
            ("Crème hydratante", 14, 48), ("Shampoing solide", 8, 18),
            ("Sérum vitamine C", 22, 60), ("Baume à lèvres", 5, 14),
            ("Huile de soin", 16, 45), ("Savon artisanal", 5, 12),
            ("Brosse à cheveux", 10, 35), ("Coffret parfum", 40, 120),
        ],
    },
    "Livres & Jeux": {
        "brands": ["Éditions du Canal", "Papier Plume", "Le Fou Blanc", "Marge & Cie"],
        "types": [
            ("Carnet pointillé", 9, 24), ("Stylo plume", 18, 80),
            ("Agenda", 12, 30), ("Roman poche", 7, 11),
            ("Livre de cuisine", 18, 35), ("Jeu d'échecs en bois", 30, 110),
            ("Jeu de cartes", 6, 18), ("Puzzle 1000 pièces", 14, 28),
        ],
    },
}

# Number of products created per product type (one per brand, brands rotate).
VARIANTS_PER_TYPE = 3

# Seasonality per category: multiplier on demand for each month, Jan..Dec.
CATEGORY_SEASONALITY = {
    "Électronique": [0.9, 0.85, 0.9, 0.9, 0.95, 0.95, 0.9, 0.95, 1.0, 1.0, 1.35, 1.5],
    "Maison & Cuisine": [1.1, 0.95, 1.0, 1.0, 1.0, 0.95, 0.9, 0.9, 1.05, 1.05, 1.1, 1.2],
    "Mode": [1.2, 0.9, 1.0, 1.05, 1.0, 1.1, 1.15, 0.85, 1.1, 1.05, 1.0, 1.1],
    "Sport & Loisirs": [1.25, 0.95, 1.0, 1.1, 1.2, 1.25, 1.2, 1.05, 1.1, 0.9, 0.85, 0.95],
    "Beauté": [0.95, 1.1, 1.0, 1.0, 1.15, 1.0, 0.95, 0.9, 0.95, 0.95, 1.05, 1.4],
    "Livres & Jeux": [0.95, 0.9, 0.9, 0.9, 0.95, 1.05, 1.1, 1.15, 1.2, 0.95, 1.1, 1.5],
}
