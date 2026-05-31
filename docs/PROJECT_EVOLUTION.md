# Project Evolution

## tg_parcer (Telegram Event Parser)

Comprehensive Telegram event parsing and enrichment system for Swyby.

### Architecture
- Python 3.12 + Gemini 3.1 Flash Image
- 4-tier deduplication (venue fuzzy, summary cross-check, Gemini verify)
- Geographic filtering (Koh Phangan only)

### Timeline
- **Feb 2026:** Core scraping, fuzzy dedup, venue enrichment
- **Mar 2026:** Gemini upgrade, geo-filters, 4-tier dedup, AI outreach

### Key Challenges
1. Multi-source duplicates
2. Ambiguous date parsing ("Today"/"Tomorrow")
3. Off-island event filtering
4. Venue name normalization

Feeds into: `koh_phangan_events`, `phangan_api`
