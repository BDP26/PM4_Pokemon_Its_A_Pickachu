import json

with open('data/silver/mappings/location_to_pokemon_map.json', 'r') as f:
    location_map = json.load(f)

# Check first location with by_version data
print("Checking location_to_pokemon_map.json for version filtering...\n")

defined_versions = {'red', 'blue', 'gold', 'silver', 'ruby', 'sapphire', 'diamond', 'pearl', 'black', 'white', 'x', 'y'}
all_versions_found = set()

for location_key, location_data in list(location_map.items())[:50]:
    if location_data.get('by_version'):
        by_version_keys = set(location_data['by_version'].keys())
        all_versions_found.update(by_version_keys)

print(f"All versions found in location_to_pokemon_map: {sorted(all_versions_found)}")
print(f"Defined versions: {sorted(defined_versions)}")

if all_versions_found.issubset(defined_versions):
    print("\n✓ SUCCESS: Only defined game versions are present!")
else:
    extra_versions = all_versions_found - defined_versions
    print(f"\n⚠ WARNING: Extra versions found: {extra_versions}")

