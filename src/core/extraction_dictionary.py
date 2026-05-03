import re

"""
ЕДИНЫЙ СПРАВОЧНИК ЭКСТРАКЦИИ ДАННЫХ (REGEX & KEYWORDS REGISTRY).
Используется парсерами и NLP-модулями. Языки: EN, GR.

AUDIT NOTE: в предыдущих ревизиях в греческих паттернах встречались
кириллические символы (визуально неотличимые от греческих). Все они
заменены на корректные греческие. Проверено побайтово.

EXPANSION 2026-05-02: добавлены паттерны для Greek Exclusive Properties
покрытия — beachfront, solarium, hotel-specific (rooms_count, beds_count,
elevator_count, buildings_count), value-amenities (pool_size_sqm,
parking_count), а также бытовые (wifi, dishwasher, washing_machine).
"""

# =====================================================================
# 1. METRICS
# =====================================================================
METRICS_PATTERNS = {
    "size_sqm": [
        # NEW: PRIORITY — Greek Exclusive hotel format "Total builded area in m2 : 8 000"
        # Must come FIRST so it captures multi-digit values containing whitespace
        # (regular space or non-breaking space \xa0) before the generic
        # "\b digits + m2 \b" pattern grabs the trailing 3 digits only.
        r"(?:total\s+builded\s+area|built\s+area|building\s+area)\s*(?:in\s+m2)?\s*[\s:\-=>]*\s*([\d][\d,.\s\xa0]*[\d])",
        r"(?:area|size|internal area|total size|living space|εμβαδόν|χώρος|επιφάνεια|καθαρά τ\.μ\.)\s*[\s:\-=>]*\s*([\d,.]+)\s*(?:sqm|sq\.?m|m2|m²|square meters?|τ\.μ\.|τμ)?",
        r"([\d,.]+)\s*(?:sqm|sq\.?m|m2|m²|square meters?|τ\.μ\.|τμ)\s*[\s:\-=>]*\s*(?:total|area|size|εμβαδόν|επιφάνεια)",
        r"(?:sized at|covering|επιφάνειας)\s*([\d,.]+)\s*(?:sqm|sq\.?m|m2|m²|τ\.μ\.|τμ)",
        r"\b([\d,.]+)\s*(?:sqm|sq\.?m|m2|m²|τ\.μ\.|τμ)\b",
        r"(?:villas?|βίλ[αες]|apartments?|διαμέρισμα|maisonettes?|μεζονέτα|house|μονοκατοικία)\s*[\s:\-=>|/]*\s*([\d,.]+)\s*(?:sqm|sq\.?m|m2|m²|τ\.μ\.|τμ)",
        r"([\d,.]+)\s*(?:sqm|sq\.?m|m2|m²|τ\.μ\.|τμ)\s*[\s:\-=>|/]*\s*(?:villas?|βίλ[αες]|apartments?|διαμέρισμα|maisonettes?|μεζονέτα|house|μονοκατοικία)",
    ],

    "land_size_sqm": [
        # NEW: PRIORITY — Greek Exclusive hotel format "Land size in m2 : 37 395"
        r"(?:land\s+size|land\s+plot)\s*(?:in\s+m2)?\s*[\s:\-=>]*\s*([\d][\d,.\s\xa0]*[\d])",
        r"(?:plot(?: size)?|land(?: size)?|lot|garden|οικόπεδο|αγροτεμάχιο|έκταση)\s*[\s:\-=>]*\s*([\d,.]+)\s*(?:sqm|sq\.?m|m2|m²|τ\.μ\.|τμ)?",
        r"([\d,.]+)\s*(?:sqm|sq\.?m|m2|m²|τ\.μ\.|τμ)\s*(?:of\s+)?(?:land|plot|garden|οικόπεδο|αγροτεμάχιο)",
        r"(?:set on|situated on|σε οικόπεδο)\s+(?:a\s+|over\s+)?([\d,.]+)\s*(?:sqm|sq\.?m|m2|m²|τ\.μ\.|τμ)?",
    ],

    "bedrooms": [
        # Number-first MUST come first to avoid grabbing the next field's
        # count in lists like "4 Bedrooms 3 Bathrooms 2 WC".
        r"\b(\d+)\s*[\s:\-]*\s*(?:bedrooms?|beds?|br|bds?|υπνοδωμάτια|υπνοδωμάτιο|υ/δ|υδ|υπν\.?)\b",
        r"(?:bedrooms?|beds?|br|bds?|υπνοδωμάτια|υπνοδωμάτιο|υ/δ|υδ|υπν\.?)\s*[\s:\-=>]*\s*(\d+)",
        r"(?:consists of|featuring|has|offers|διαθέτει|αποτελείται από)\s*(\d+)\s*(?:bedrooms?|beds?|υ/δ|υπνοδωμάτια)"
    ],

    "bathrooms": [
        # Number-first MUST come first (same fix as bedrooms above).
        r"\b(\d+)\s*[\s:\-]*\s*(?:bathrooms?|baths?|ba|μπάνια|μπάνιο|λουτρά|wc)\b",
        r"(?:bathrooms?|baths?|ba|μπάνια|μπάνιο|λουτρά|wc)\s*[\s:\-=>]*\s*(\d+)",
        r"(?:and|with|με)\s*(\d+)\s*(?:bathrooms?|baths?|μπάνια|λουτρά|wc)"
    ],

    "levels": [
        r"(?:levels?|floors?|storeys?|επίπεδα|επίπεδο|όροφοι)\s*[\s:\-=>]*\s*(\d+)",
        r"(\d+)\s*[- ]*\s*(?:levels?|storey|floors?|επίπεδα|επίπεδο|όροφοι)",
        r"(?:across|over|on|in|σε)\s+(\d+)\s+(?:levels?|floors?|επίπεδα|ορόφους)",
        # NEW: Greek Exclusive hotel "Max No. of Levels (floors): 3 levels"
        r"(?:no\.?\s+of\s+(?:levels|floors)|number\s+of\s+(?:levels|floors))\s*(?:\(floors\))?\s*[\s:\-=>]*\s*(\d+)",
    ],

    "year_built": [
        r"(?:built|constructed|construction year|year built|έτος κατασκευής|κατασκευής)\s*[\s:\-=>]*\s*(19\d{2}|20\d{2})",
        r"(?:built in|constructed in|completed in|κατασκευάστηκε το|το έτος)\s*(19\d{2}|20\d{2})",
        r"\b(19[5-9]\d|20[0-2]\d)\b",
        # NEW: Greek Exclusive hotel "Built in / year: 1992"
        r"(?:built\s+in\s*/\s*year|year\s+of\s+construction)\s*[\s:\-=>]*\s*(19\d{2}|20\d{2})",
    ],

    "distance_to_sea": [
        r"(?:distance(?: to sea)?|to the beach|from the sea|from beach|απόσταση(?: από θάλασσα)?|από την παραλία)\s*[\s:\-=>]*\s*([\d,.]+)\s*(?:m|meters?|km|kilometers?|μ\.?|μέτρα|χλμ)",
        r"([\d,.]+)\s*(?:m|meters?|km|μ\.?|μέτρα|χλμ)\s*(?:from|to|από)\s*(?:the\s+)?(?:sea|beach|coast|θάλασσα|παραλία)",
        r"(?:only|just|μόλις)\s*([\d,.]+)\s*(?:m|meters?|km|μ\.?|μέτρα|χλμ)\s*(?:away|από)?",
        # NEW: Greek Exclusive Highlights "Beachfront location — 10 meters from the sea"
        r"(?:beachfront\s+location[\s\-—–]*)([\d,.]+)\s*(?:m|meters?|μ)\s*(?:from\s+the\s+sea)?",
    ],

    # ========== NEW METRICS (Greek Exclusive coverage) ==========

    "pool_size_sqm": [
        # "32 m² swimming pool", "32m² pool with wooden sun deck", "pool of 50 sqm"
        r"\b([\d,.]+)\s*(?:sqm|sq\.?m|m2|m²|τ\.μ\.|τμ)\s*(?:swimming\s+)?pool\b",
        r"\bpool\s*(?:size)?\s*(?:of)?\s*[\s:\-=>]*\s*([\d,.]+)\s*(?:sqm|sq\.?m|m2|m²)",
        r"\b(?:swimming\s+pool|πισίνα)\s+(?:of\s+)?([\d,.]+)\s*(?:sqm|sq\.?m|m2|m²|τ\.μ\.|τμ)",
    ],

    "parking_count": [
        # "6 covered parking lots", "3 parking spots", "two parking spaces"
        r"\b(\d+)\s+(?:covered\s+)?(?:parking\s+(?:lots?|spots?|spaces?|places?)|θέσεις\s+(?:στάθμευσης|πάρκινγκ))",
        r"(?:parking|στάθμευση|πάρκινγκ)\s*[\s:\-=>]*\s*(\d+)\s*(?:lots?|spots?|spaces?|θέσε[ιω]ς)?",
        # NEW: "4 private parking", "3 outdoor parking", "2 underground parking"
        # Number BEFORE "parking" with adjective in between, no "spots/lots" suffix.
        r"\b(\d+)\s+(?:private|covered|outdoor|underground|indoor|reserved)\s+parking\b",
        # NEW: "private parking for 4 vehicles", "parking for 6 cars"
        r"\bparking\s+for\s+(\d+)\s+(?:cars?|vehicles?|αυτοκίνητα)\b",
    ],

    "elevator_count": [
        # "No. of elevators: 3 elevators"
        r"(?:no\.?\s+of\s+elevators?|number\s+of\s+elevators?)\s*[\s:\-=>]*\s*(\d+)",
        r"\b(\d+)\s+elevators?\b",
    ],

    "buildings_count": [
        # "No. of buildings: 9 Buildings"
        r"(?:no\.?\s+of\s+buildings?|number\s+of\s+buildings?)\s*[\s:\-=>]*\s*(\d+)",
        r"\b(\d+)\s+buildings?\b(?!\s*area)",  # Avoid match "buildings area"
    ],

    "rooms_count": [
        # Hotel: "139 rooms", "139 rooms of which 15 are luxury suites"
        r"\b(\d+)\s+rooms?\s+(?:of\s+which|in\s+total|total)",
        r"(?:total\s+(?:no\.?\s+of\s+)?rooms?|number\s+of\s+rooms?)\s*[\s:\-=>]*\s*(\d+)",
    ],

    "living_rooms_count": [
        # "Living Rooms: 2", "2 living rooms", "No. of living rooms: 2"
        r"\bliving\s+rooms?\s*[\s:\-=>]+\s*(\d+)",
        r"\b(\d+)\s+living\s+rooms?\b",
        r"(?:no\.?\s+of\s+living\s+rooms?|number\s+of\s+living\s+rooms?)\s*[\s:\-=>]*\s*(\d+)",
    ],

    "kitchens_count": [
        # "Kitchens: 3", "3 kitchens", "No. of kitchens: 3"
        r"\bkitchens?\s*[\s:\-=>]+\s*(\d+)",
        r"\b(\d+)\s+kitchens?\b",
        r"(?:no\.?\s+of\s+kitchens?|number\s+of\s+kitchens?)\s*[\s:\-=>]*\s*(\d+)",
    ],

    "beds_count": [
        # Hotel: "Number of Beds: min 287 – max 387 Beds", "287 beds total"
        r"(?:number\s+of\s+beds?|no\.?\s+of\s+beds?)\s*[\s:\-=>]*\s*(?:min\s+)?(\d+)",
        r"\b(\d+)\s+beds?\s+(?:total|in\s+total|capacity|max)",
    ],

    "renovation_year": [
        # "Last Renovation / year: 2002", "Renovated in 2018"
        r"(?:last\s+renovation\s*/\s*year|renovation\s+year|year\s+of\s+renovation)\s*[\s:\-=>]*\s*(19\d{2}|20\d{2})",
        r"(?:renovated|refurbished|ανακαινισμένο|ανακαινίσθηκε)\s+(?:in\s+|the\s+year\s+|το\s+)?(19\d{2}|20\d{2})",
    ],
}

# =====================================================================
# 2. PROPERTY TYPES
# =====================================================================
# ORDER MATTERS: extract_type() uses first-match-wins. Order chosen so:
#   * Most-specific listings (Hotel) win first — hotel descriptions often
#     mention "...and one Villa" referring to a suite, which would otherwise
#     mis-classify the whole listing as Villa.
#   * Land/Plot LAST — every villa listing mentions "land plot: NNNm²" or
#     "covers 3500 sqm of land", which would falsely classify every villa
#     as Land/Plot if checked first.
PROPERTY_TYPES = {
    "Hotel/Commercial": [r"\b(?:hotels?|ξενοδοχεί[οα]|commercial|επαγγελματικός χώρος)\b"],
    "Maisonette":       [r"\b(?:maisonettes?|μεζονέτ[αες]|mezonet[aes])\b"],
    "Townhouse":        [r"\b(?:townhouses?|συγκρότημα(?: κατοικιών)?)\b"],
    "Detached House":   [r"\b(?:detached houses?|μονοκατοικία|single family homes?)\b"],
    "Apartment":        [r"\b(?:apartments?|flats?|διαμέρισμα(?:τα)?)\b"],
    "Villa":            [r"\b(?:villas?|βίλ[αες]|βιλ[αες])\b"],
    # Studio AFTER Villa — descriptions sometimes mention "1 studio with..."
    # as a room inside a villa, which would otherwise mis-classify the
    # whole listing as Studio.
    "Studio":           [r"\b(?:studios?|στούντιο|γκαρσονιέρα)\b"],
    "Land/Plot":        [r"\b(?:plots?|lands?|parcels?|οικόπεδ[οα]|αγροτεμάχι[οα])\b"],
}

# =====================================================================
# 3. EXTRA FEATURES
# =====================================================================
EXTRA_FEATURES_PATTERNS = {
    "helipad":           [r"\b(?:helipad|heliport|helicopter landing pad|ελικοδρόμιο)\b"],
    "private_marina":    [r"\b(?:private marina|private dock|pier|boat slip|ιδιωτική μαρίνα|μαρίνα|προβλήτα)\b"],
    "swimming_pool":     [r"\b(?:pool|swimming pool|private pool|shared pool|πισίνα|ιδιωτική πισίνα)\b"],
    "parking":           [r"\b(?:parking|garage|carport|πάρκινγκ|στάθμευση|γκαράζ)\b"],
    "air_conditioning":  [r"\b(?:a/c|air condition(?:ing)?|klima|κλιματισμός|κλιματιστικό|aircon|air-conditioned)\b"],
    # FIXED: θέρманση -> θέρμανση (mu was Cyrillic)
    "heating":           [r"\b(?:heating|central heating|underfloor heating|θέρμανση|ενδοδαπέδια|καλοριφέρ|heat pump|αντλία θερμότητας)\b"],
    "fireplace":         [r"\b(?:fireplaces?|τζάκι(?:α)?)\b"],
    "furnished":         [r"\b(?:fully furnished|furnished|επιπλωμένο|πλήρως επιπλωμένο)\b"],
    "alarm_system":      [r"\b(?:alarm|security system|cctv|συναγερμός|κάμερες|σύστημα ασφαλείας)\b"],
    "garden":            [r"\b(?:gardens?|landscaped garden|yard|κήπος|αυλή)\b"],
    "bbq":               [r"\b(?:bbq|barbeque|barbecue|μπάρμπεκιου|ψησταριά)\b"],
    "solar_panels":      [r"\b(?:solar(?: panels?)?|solar water heater|ηλιακός|ηλιακός θερμοσίφωνας)\b"],
    "sea_view":          [r"\b(?:sea view|ocean view|panoramic sea view|view to the sea|view of the sea|θέα θάλασσα|απεριόριστη θέα)\b"],
    "elevator":          [r"\b(?:elevators?|lifts?|ασανσέρ|ανελκυστήρας)\b"],
    "storage_room":      [r"\b(?:storage(?: room)?|αποθήκη)\b"],
    "playroom":          [r"\b(?:playrooms?|πλέιρουμ|play room)\b"],
    "renovated":         [r"\b(?:renovated|recently renovated|refurbished|ανακαινισμένο|πλήρως ανακαινισμένο)\b"],
    "smart_home":        [r"\b(?:smart home|έξυπνο σπίτι|home automation)\b"],
    # FIXED: γυмναστήριο -> γυμναστήριο (mu was Cyrillic)
    "gym":               [r"\b(?:gym|fitness room|γυμναστήριο)\b"],
    "jacuzzi_sauna":     [r"\b(?:jacuzzi|sauna|hammam|τζακούζι|σάουνα|χαμάμ)\b"],
    "mosquito_nets":     [r"\b(?:mosquito nets?|fly screens?|window screens?|σίτες?|σιτες?)\b"],
    "awnings":           [r"\b(?:awnings?|sunshades?|τέντες?|τεντες?)\b"],
    "security_door":     [r"\b(?:security door|armored door|πόρτα ασφαλείας|θωρακισμένη πόρτα)\b"],
    "water_well":        [r"\b(?:water well|drilling|borehole|γεώτρηση|well\b)\b"],

    # --- EXTRA LUXURY -----------------------------------------------
    "wine_cellar":       [r"\b(?:wine cellar|wine room|cava|κάβα|κελάρι)\b"],
    "home_cinema":       [r"\b(?:home cinema|movie room|home theater|σινεμά|home theatre)\b"],
    "staff_quarters":    [r"\b(?:maid's room|staff quarters|service room|staff room|δωμάτιο υπηρεσίας|δωμάτιο προσωπικού)\b"],
    "infinity_pool":     [r"\b(?:infinity pool|overflow pool|υπερχείλιση|πισίνα υπερχείλισης)\b"],
    # FIXED: μπάσκет -> μπάσκετ (epsilon+tau were Cyrillic)
    "tennis_court":      [r"\b(?:tennis court|basketball court|padel court|sports court|γήπεδο τένις|γήπεδο μπάσκετ)\b"],
    "private_beach":     [r"\b(?:private beach|direct access to (?:the )?sea|front line|ιδιωτική παραλία|πρώτο στη θάλασσα|άμεση πρόσβαση στη θάλασσα)\b"],
    "walk_in_closet":    [r"\b(?:walk-in closet|dressing room|walk in closet|βεστιάριο)\b"],
    "ev_charger":        [r"\b(?:ev charger|electric car charging|φόρτιση ηλεκτρικού αυτοκινήτου|wallbox)\b"],
    "guest_house":       [r"\b(?:guest house|independent guest house|guesthouse|ξενώνας|αυτόνομος ξενώνας)\b"],
    "smart_locking":     [r"\b(?:fingerprint lock|keyless entry|smart lock|έξυπνη κλειδαριά)\b"],
    "indoor_pool":       [r"\b(?:indoor pool|heated pool|εσωτερική πισίνα|θερμαινόμενη πισίνα)\b"],
    "landscape_design":  [r"\b(?:landscaped garden|automatic irrigation|botanical garden|αυτόματο πότισμα|αρχιτεκτονική τοπίου)\b"],

    # ========== NEW: Greek Exclusive coverage ==========

    # Beachfront — distinct from private_beach (which requires direct access).
    # Beachfront = villa is on the first row from the sea (visual / location).
    "beachfront":        [r"\b(?:beachfront|sea\s*front|seafront|first line of (?:the )?beach|just in front of the beach|on the (?:water|sea)front|by the sea\b|on the beach)\b"],

    # Solarium — distinct wellness amenity (often separate from sauna)
    "solarium":          [r"\b(?:solariums?|σολάριουμ|σολαριουμ)\b"],

    # Balcony / Terrace — extremely common, currently missed
    "balcony":           [r"\b(?:balcon(?:y|ies)|μπαλκόνι|βεραντούλα)\b"],
    "terrace":           [r"\b(?:terraces?|covered terraces?|βεράντα|ταράτσα)\b"],

    # Connectivity & utilities — household basics
    "wifi":              [r"\b(?:wi-?fi|wireless internet|wlan|ασύρματο\s+(?:internet|δίκτυο))\b"],
    "tv_satellite":      [r"\b(?:satellite tv|sat tv|sky\s*tv|δορυφορική τηλεόραση)\b"],
    "dishwasher":        [r"\b(?:dishwashers?|machine for dishes|πλυντήριο πιάτων)\b"],
    "washing_machine":   [r"\b(?:washing machines?|automatic washing|laundry machines?|πλυντήρ[ιί][οα] ρούχων)\b"],

    # Outdoor amenities
    "sun_deck":          [r"\b(?:sun ?decks?|wooden deck|sundeck|ξύλινο\s+ντεκ)\b"],
    "outdoor_dining":    [r"\b(?:outdoor dining|covered dining area|al fresco dining|outdoor (?:lounge|seating))\b"],

    # Hotel-specific (only fire on actual hotel listings)
    "hotel_restaurant":  [r"\b(?:on-?site restaurant|hotel restaurant|hotel taverna|in-?house restaurant)\b"],
    "hotel_bar":         [r"\b(?:pool bar|beach bar|lobby bar|hotel bar)\b"],
    "conference_center": [r"\b(?:conference (?:center|centre|rooms?|hall)|αίθουσα συνεδρίων|meeting rooms?)\b"],
    "spa":               [r"\b(?:spa\s*(?:center|centre|facilities)?|wellness center|wellness centre)\b"],

    # ========== NEW: more amenities + flooring/outdoor markers ==========

    # Pergola — extremely common on Halkidiki villas with outdoor space
    "pergola":           [r"\b(?:pergolas?|πέργκολα)\b"],

    # Outdoor WC — appears on luxury villas with pool houses
    "outdoor_wc":        [r"\b(?:outdoor|external|exterior)\s+(?:wc|toilet|bathroom)\b"],

    # Flooring type — luxury markers, separate flags so they're queryable.
    # Patterns match either "marble floor(s)/flooring/tiles" anywhere, OR
    # "Flooring Type: ... marble ..." (handles list-style spec sheets).
    "marble_floor":      [r"\bmarble\s+(?:floors?|flooring|tiles?)\b|\bflooring[^.\n]{0,40}marble\b"],
    "wooden_floor":      [r"\b(?:wooden?|hardwood|parquet)\s+(?:floors?|flooring)\b|\bflooring[^.\n]{0,40}(?:wood|parquet)\b"],
}

# =====================================================================
# 4. SYSTEM DATA
# =====================================================================
SYSTEM_PATTERNS = {
    "site_property_id": [
        r"(?:Property ID|Listing ID|Ref(?:erence)?\s*(?:No\.?)?|ID|Κωδικός Ακινήτου|Κωδ\.)\s*[:\-#]?\s*([A-Za-z0-9_-]+)"
    ],
    "last_updated": [
        r"(?:Last updated|Updated(?: on)?|Modified|Ημερομηνία ενημέρωσης|Ενημερώθηκε)\s*[:\-]?\s*([\d./-]+)"
    ]
}