// FanDuel Sportsbook Scraper Module
// Fetches player props from FanDuel's public API (no auth/proxy needed)

const fetch = require('node-fetch');

const BASE_URL = 'https://sbapi.mi.sportsbook.fanduel.com';
const API_KEY = 'FhMFpcPWXMeyZxOx';

// Sport slugs → canonical sport IDs
const SPORT_SLUGS = {
  nba: 'NBA',
  nfl: 'NFL',
  mlb: 'MLB',
  nhl: 'NHL',
  ncaab: 'CBB',
};

// Tabs to scrape per event for player props
const PLAYER_TABS = [
  'player-points',
  'player-rebounds',
  'player-assists',
  'player-threes',
  'player-props',
];

// Market name → stat type mapping
function parseStatType(marketName, marketType) {
  const mn = (marketName || '').toLowerCase();
  const mt = (marketType || '').toLowerCase();

  // Over/Under style markets
  if (mn.includes('points') && !mn.includes('rebound') && !mn.includes('assist') && !mn.includes('three') && !mn.includes('total points')) return 'Points';
  if (mn.includes('rebound')) return 'Rebounds';
  if (mn.includes('assist')) return 'Assists';
  if (mn.includes('three') || mn.includes('3-pointer') || mn.includes('3 pt') || mn.includes('made threes')) return '3-Pointers Made';
  if (mn.includes('pts + reb + ast') || mn.includes('pts+reb+ast')) return 'Pts + Rebs + Asts';
  if (mn.includes('pts + reb') || mn.includes('pts+reb')) return 'Points + Rebounds';
  if (mn.includes('pts + ast') || mn.includes('pts+ast')) return 'Points + Assists';
  if (mn.includes('reb + ast') || mn.includes('reb+ast')) return 'Rebounds + Assists';
  if (mn.includes('steal')) return 'Steals';
  if (mn.includes('block')) return 'Blocks';
  if (mn.includes('turnover')) return 'Turnovers';
  if (mn.includes('passing yard')) return 'Passing Yards';
  if (mn.includes('rushing yard')) return 'Rushing Yards';
  if (mn.includes('receiving yard')) return 'Receiving Yards';
  if (mn.includes('passing td') || mn.includes('pass td')) return 'Passing TDs';
  if (mn.includes('reception')) return 'Receptions';
  if (mn.includes('strikeout')) return 'Strikeouts';
  if (mn.includes('hit') && !mn.includes('shot')) return 'Hits';
  if (mn.includes('goal') && !mn.includes('total')) return 'Goals';
  if (mn.includes('save')) return 'Saves';
  if (mn.includes('shot')) return 'Shots on Goal';

  // "To Score X+" style (common in All-Star, etc)
  const scoreMatch = mn.match(/to (?:score|record|get) (\d+)\+?\s+(.+)/);
  if (scoreMatch) return scoreMatch[2].trim().replace(/\b\w/g, c => c.toUpperCase());

  return marketName || 'Unknown';
}

// Check if a market is an over/under player prop
function isOverUnderMarket(market) {
  const runners = market.runners || [];
  return runners.some(r => r.result?.type === 'OVER') && runners.some(r => r.result?.type === 'UNDER');
}

// Convert American odds to decimal
function americanToDecimal(odds) {
  if (odds == null || odds === 0) return null;
  if (odds < 0) return 1 + (100 / Math.abs(odds));
  return 1 + (odds / 100);
}

async function fetchJSON(url) {
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const resp = await fetch(url, {
        headers: { 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36' },
        timeout: 30000,
      });
      if (!resp.ok) {
        if (resp.status === 429) {
          await new Promise(r => setTimeout(r, 2000 * attempt));
          continue;
        }
        throw new Error(`HTTP ${resp.status}`);
      }
      return await resp.json();
    } catch (err) {
      if (attempt === 3) throw err;
      await new Promise(r => setTimeout(r, 1000 * attempt));
    }
  }
}

async function fetchEvents(sportSlug) {
  const url = `${BASE_URL}/api/content-managed-page?page=CUSTOM&customPageId=${sportSlug}&_ak=${API_KEY}`;
  const data = await fetchJSON(url);
  const events = data.attachments?.events || {};

  // Filter to actual games (has " @ " or " At " or " v " in name, or has a future openDate)
  const gameEvents = [];
  for (const [id, evt] of Object.entries(events)) {
    const name = evt.name || '';
    // Skip futures, specials, awards, draft markets
    if (/futures|specials|awards|draft|championship/i.test(name) && !/ @ | At | v /i.test(name)) continue;
    gameEvents.push({ eventId: id, name: evt.name, openDate: evt.openDate });
  }
  return gameEvents;
}

async function fetchEventProps(eventId) {
  const allMarkets = {};
  for (const tab of PLAYER_TABS) {
    try {
      const url = `${BASE_URL}/api/event-page?_ak=${API_KEY}&tab=${tab}&eventId=${eventId}`;
      const data = await fetchJSON(url);
      const markets = data.attachments?.markets || {};
      Object.assign(allMarkets, markets);
    } catch (err) {
      // Some tabs may not exist for certain events, that's fine
    }
    // Small delay between tabs to be polite
    await new Promise(r => setTimeout(r, 200));
  }
  return allMarkets;
}

function processMarkets(markets, sportId, gameDisplay, eventId) {
  const props = [];
  const now = new Date().toISOString();

  for (const [marketId, market] of Object.entries(markets)) {
    const marketName = market.marketName || '';
    const marketType = market.marketType || '';
    const runners = market.runners || [];

    // Skip non-player markets (game lines, totals, moneylines)
    if (/^(spread betting|moneyline|total (?:points|goals)|puckline|will there|game to reach)/i.test(marketName)) continue;
    if (marketType === 'MATCH_HANDICAP_(2-WAY)' || marketType === 'MONEY_LINE') continue;
    if (marketType === 'TOTAL_POINTS_(OVER/UNDER)' && !marketName.toLowerCase().includes('player')) continue;

    const statType = parseStatType(marketName, marketType);

    if (isOverUnderMarket(market)) {
      // Over/Under player prop — runners come in pairs
      const overRunner = runners.find(r => r.result?.type === 'OVER');
      const underRunner = runners.find(r => r.result?.type === 'UNDER');
      if (!overRunner || !underRunner) continue;

      const line = overRunner.handicap;
      if (line == null) continue;

      // The runner name for O/U is just "Over"/"Under", player name is in the market name
      const playerMatch = marketName.match(/^(.+?)(?:\s+[-–]\s+|\s+Over\/Under|\s+O\/U|\s+Total)/i);
      const playerName = playerMatch ? playerMatch[1].trim() : marketName;
      if (!playerName || playerName === 'Over' || playerName === 'Under') continue;

      const overOdds = overRunner.winRunnerOdds?.americanDisplayOdds?.americanOdds;
      const underOdds = underRunner.winRunnerOdds?.americanDisplayOdds?.americanOdds;

      props.push({
        id: `fd-${marketId}`,
        player_name: playerName,
        sport_id: sportId,
        stat_type: statType,
        stat_value: line,
        over_price: overOdds != null ? String(overOdds) : null,
        under_price: underOdds != null ? String(underOdds) : null,
        over_decimal: americanToDecimal(overOdds),
        under_decimal: americanToDecimal(underOdds),
        game_display: gameDisplay,
        source: 'fanduel',
        external_id: marketId,
        updated_at: now,
      });
    } else {
      // "To Score X+" style or other player prop markets
      // Each runner is a different player, all with the same threshold
      const thresholdMatch = marketName.match(/(\d+)\+/);
      const threshold = thresholdMatch ? parseFloat(thresholdMatch[1]) : null;

      for (const runner of runners) {
        if (runner.runnerStatus !== 'ACTIVE') continue;
        const playerName = runner.runnerName;
        if (!playerName) continue;

        const odds = runner.winRunnerOdds?.americanDisplayOdds?.americanOdds;

        props.push({
          id: `fd-${marketId}-${runner.selectionId}`,
          player_name: playerName,
          sport_id: sportId,
          stat_type: statType,
          stat_value: threshold,
          over_price: odds != null ? String(odds) : null,
          under_price: null,
          over_decimal: americanToDecimal(odds),
          under_decimal: null,
          game_display: gameDisplay,
          source: 'fanduel',
          external_id: `${marketId}-${runner.selectionId}`,
          updated_at: now,
        });
      }
    }
  }

  return props;
}

// Main hydration function — called by index.js with supabase client
async function hydrate(log) {
  log = log || console.log;
  const allProps = [];
  const errors = [];

  for (const [slug, sportId] of Object.entries(SPORT_SLUGS)) {
    try {
      const events = await fetchEvents(slug);
      log(`[FD] ${slug.toUpperCase()}: ${events.length} events found`);

      for (const event of events) {
        try {
          const markets = await fetchEventProps(event.eventId);
          const marketCount = Object.keys(markets).length;
          if (marketCount === 0) continue;

          const props = processMarkets(markets, sportId, event.name, event.eventId);
          if (props.length > 0) {
            log(`[FD] ${event.name}: ${props.length} props from ${marketCount} markets`);
            allProps.push(...props);
          }
        } catch (err) {
          errors.push({ event: event.name, error: err.message });
        }

        // Delay between events
        await new Promise(r => setTimeout(r, 300));
      }
    } catch (err) {
      errors.push({ sport: slug, error: err.message });
      log(`[FD] Error fetching ${slug}: ${err.message}`);
    }
  }

  log(`[FD] Total: ${allProps.length} props scraped (${errors.length} errors)`);
  return allProps;
}

module.exports = { hydrate };
