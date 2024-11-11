# unofficial_gugik_stac
Narzędzia do wygenerowania nieoficjalnego Katalogu STAC z danymi publicznie udostępnianymi przez GUGiK

Przykłady komend:
```bash
cd ggkstac

# pobierz biblioteki
uv sync --freeze

# pobierz dane
uv run ggkstac download_all --output-folder ./data

# przekonwertuj pobrane dane do katalogu stac
uv run ggkstac --log-level INFO convert_geoparquet --input-folder ./data --output-folder ./dist_stac
```
