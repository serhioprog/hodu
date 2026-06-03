-- Sprint 12: Halkidiki refdata expansion — 4 villages found via Nominatim
-- Spitogatos uses these area names; original location_areas seed didn't include them.
-- Verified via OpenStreetMap Nominatim API + Halkidiki bbox sanity check.

INSERT INTO location_areas (country_id, prefecture_id, municipality_id, area_en, area_el, lat, lng) VALUES
  (1, 1, 4, 'Glifoneri',          'Γλυφονέρι',           39.9800, 23.3759),
  (1, 1, 1, 'Louki',               'Λούκι',               40.3851, 23.4424),
  (1, 1, 4, 'Filakes Kassandras', 'Φύλακες Κασσάνδρας',  40.1142, 23.3392),
  (1, 1, 5, 'Zografou',            'Ζωγράφου',            40.2862, 23.2546)
ON CONFLICT DO NOTHING;
