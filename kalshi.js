// Kalshi API scraper module
// Fetches sports markets from Kalshi and normalizes into our props format

// Use global fetch if available (Node 18+), else require node-fetch
const fetchFn = typeof globalThis.fetch === 'function' ? globalThis.fetch : require('node-fetch');

const KALSHI_API = 'https://api.elections.kalshi.com/trade-api/v2';

// Series tickers for sports we care about (player props + game lines)
const PLAYER_PROP_SERIES = {
  // NBA player props
  KXNBAPTS: { sport: 'NBA', stat: 'Points' },
  KXNBAREB: { sport: 'NBA', stat: 'Rebounds' },
  KXNBAAST: { sport: 'NBA', stat: 'Assists' },
  KXNBA3PT: { sport: 'NBA', stat: '3-Pointers Made' },
  KXNBAPRA: { sport: 'NBA', stat: 'Pts + Rebs + Asts' },
  KXNBAPR: { sport: 'NBA', stat: 'Points + Rebounds' },
  KXNBAPA: { sport: 'NBA', stat: 'Points + Assists' },
  KXNBARA: { sport: 'NBA', stat: 'Rebounds + Assists' },
  KXNBASTL: { sport: 'NBA', stat: 'Steals' },
  KXNBABLK: { sport: 'NBA', stat: 'Blocks' },
};

const GAME_LINE_SERIES = {
  // NBA game lines
  KXNBAGAME: { sport: 'NBA', stat: 'Moneyline' },
  KXNBASPREAD: { sport: 'NBA', stat: 'Spread' },
  KXNBATOTAL: { sport: 'NBA', stat: 'Total' },
  // CBB game lines
  KXNCAAMBGAME: { sport: 'CBB', stat: 'Moneyline' },
  KXNCAAMBSPREAD: { sport: 'CBB', stat: 'Spread' },
  KXNCAAMBTOTAL: { sport: 'CBB', stat: 'Total' },
  // NHL game lines
  KXNHLGAME: { sport: 'NHL', stat: 'Moneyline' },
  // UFC
  KXUFCFIGHT: { sport: 'MMA', stat: 'Fight Winner' },
};

// Convert Kalshi cents (0-100) to American odds
function centsToAmerican(cents) {
  if (!cents || cents <= 0 || cents >= 100) return null;
  const impliedProb = cents / 100;
  if (impliedProb >= 0.5) {
    return Math.round(-100 * impliedProb / (1 - impliedProb)).toString();
  } else {
    return `+${Math.round(100 * (1 - impliedProb) / impliedProb)}`;
  }
}

// Extract player name from Kalshi market title
// e.g., "Will LeBron James score over 25.5 points?" -> "LeBron James"
function extractPlayerName(title, yesSub) {
  if (yesSub) return yesSub;
  // Try to extract from "Will [Name] ..." pattern
  const match = title.match(/^Will (.+?) (?:score|get|have|record|win|make)/i);
  if (match) return match[1];
  return title;
}

// Extract line value from ticker or title
// e.g., KXNBAPTS-26FEB20LEBRON-25 -> 25.5
function extractLineFromTicker(ticker) {
  const parts = ticker.split('-');
  const last = parts[parts.length - 1];
  const num = parseFloat(last);
  if (!isNaN(num)) return num + 0.5; // Kalshi uses integers for O/U lines, actual line is +0.5
  return null;
}

// Extract line from title
function extractLineFromTitle(title) {
  const match = title.match(/over (\d+\.?\d*)/i) || title.match(/(\d+\.?\d*)\+/);
  if (match) return parseFloat(match[1]);
  return null;
}

async function fetchWithRetry(url, retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      const resp = await fetchFn(url, {
        headers: {
          'Accept': 'application/json',
          'User-Agent': 'STACKED-Props-Dashboard/1.0'
        }
      });
      if (resp.status === 429) {
        // Rate limited - wait and retry
        const wait = Math.pow(2, i) * 2000;
        await new Promise(r => setTimeout(r, wait));
        continue;
      }
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      return await resp.json();
    } catch (err) {
      if (i === retries - 1) throw err;
      await new Promise(r => setTimeout(r, 1000 * (i + 1)));
    }
  }
}

// Fetch all markets for a series
async function fetchSeriesMarkets(seriesTicker, limit = 200) {
  const allMarkets = [];
  let cursor = null;

  do {
    const url = `${KALSHI_API}/markets?limit=${limit}&status=open&series_ticker=${seriesTicker}${cursor ? `&cursor=${cursor}` : ''}`;
    const data = await fetchWithRetry(url);
    const markets = data.markets || [];
    allMarkets.push(...markets);
    cursor = markets.length === limit ? data.cursor : null;
  } while (cursor);

  return allMarkets;
}

// Fetch events with nested markets for better context
async function fetchSeriesEvents(seriesTicker) {
  const allEvents = [];
  let cursor = null;

  do {
    const url = `${KALSHI_API}/events?limit=100&status=open&with_nested_markets=true&series_ticker=${seriesTicker}${cursor ? `&cursor=${cursor}` : ''}`;
    const data = await fetchWithRetry(url);
    const events = data.events || [];
    allEvents.push(...events);
    cursor = events.length === 100 ? data.cursor : null;
  } while (cursor);

  return allEvents;
}

// Process player prop events into normalized props
function processPlayerPropEvents(events, seriesConfig) {
  const props = [];

  for (const event of events) {
    const markets = event.markets || [];
    if (markets.length === 0) continue;

    // For player props, each event is one player, markets are different lines
    // Find the market closest to "even" (yes_bid ~50) for the primary line
    // Also get the most liquid market
    const activeMarkets = markets.filter(m => m.status === 'active' || m.status === 'open');
    if (activeMarkets.length === 0) continue;

    // Sort by liquidity/volume
    activeMarkets.sort((a, b) => (b.volume || 0) - (a.volume || 0));

    // For each market in the event, create a prop
    // The "best" line is the one closest to 50/50
    let bestMarket = activeMarkets[0];
    let bestDist = Infinity;
    for (const m of activeMarkets) {
      const mid = ((m.yes_bid || 0) + (m.yes_ask || 0)) / 2;
      const dist = Math.abs(mid - 50);
      if (dist < bestDist) {
        bestDist = dist;
        bestMarket = m;
      }
    }

    const lineValue = extractLineFromTicker(bestMarket.ticker) ||
                      extractLineFromTitle(bestMarket.title) ||
                      extractLineFromTitle(event.title);

    const playerName = bestMarket.yes_sub_title || extractPlayerName(event.title, null);
    const gameDisplay = event.sub_title || event.title;

    // yes_bid = over price, no_bid = under price
    const overPrice = centsToAmerican(bestMarket.yes_bid);
    const underPrice = centsToAmerican(100 - (bestMarket.yes_ask || 0)); // under ≈ no side

    props.push({
      id: `kalshi_${bestMarket.ticker}`,
      appearance_id: null,
      player_id: null,
      game_id: event.event_ticker,
      sport_id: seriesConfig.sport,
      stat_type: seriesConfig.stat,
      stat_display: seriesConfig.stat,
      stat_value: lineValue,
      over_price: overPrice,
      under_price: underPrice,
      over_decimal: bestMarket.yes_bid ? (bestMarket.yes_bid / 100) : null,
      under_decimal: bestMarket.no_bid ? (bestMarket.no_bid / 100) : null,
      line_type: 'binary',
      status: 'active',
      choice_display: playerName,
      player_name: playerName,
      team_abbr: null,
      game_display: gameDisplay,
      source: 'kalshi',
      updated_at: bestMarket.updated_time || new Date().toISOString()
    });
  }

  return props;
}

// Process game line events
function processGameLineEvents(events, seriesConfig) {
  const props = [];

  for (const event of events) {
    const markets = event.markets || [];
    if (markets.length === 0) continue;

    if (seriesConfig.stat === 'Moneyline' || seriesConfig.stat === 'Fight Winner') {
      // Each market is a side (team/fighter)
      for (const market of markets) {
        if (market.status !== 'active') continue;
        const name = market.yes_sub_title || market.title;
        const overPrice = centsToAmerican(market.yes_bid);

        props.push({
          id: `kalshi_${market.ticker}`,
          appearance_id: null,
          player_id: null,
          game_id: event.event_ticker,
          sport_id: seriesConfig.sport,
          stat_type: seriesConfig.stat,
          stat_display: seriesConfig.stat,
          stat_value: market.yes_bid || 0,
          over_price: overPrice,
          under_price: null,
          over_decimal: market.yes_bid ? (market.yes_bid / 100) : null,
          under_decimal: null,
          line_type: 'moneyline',
          status: 'active',
          choice_display: name,
          player_name: name,
          team_abbr: null,
          game_display: event.sub_title || event.title,
          source: 'kalshi',
          updated_at: market.updated_time || new Date().toISOString()
        });
      }
    } else if (seriesConfig.stat === 'Total' || seriesConfig.stat === 'Spread') {
      // Multiple strike markets - pick the one closest to even
      const activeMarkets = markets.filter(m => m.status === 'active');
      let bestMarket = activeMarkets[0];
      let bestDist = Infinity;
      for (const m of activeMarkets) {
        const mid = ((m.yes_bid || 0) + (m.yes_ask || 0)) / 2;
        const dist = Math.abs(mid - 50);
        if (dist < bestDist) {
          bestDist = dist;
          bestMarket = m;
        }
      }
      if (!bestMarket) continue;

      const lineValue = extractLineFromTicker(bestMarket.ticker) ||
                        extractLineFromTitle(bestMarket.title) ||
                        extractLineFromTitle(event.title);

      props.push({
        id: `kalshi_${bestMarket.ticker}`,
        appearance_id: null,
        player_id: null,
        game_id: event.event_ticker,
        sport_id: seriesConfig.sport,
        stat_type: seriesConfig.stat,
        stat_display: seriesConfig.stat,
        stat_value: lineValue,
        over_price: centsToAmerican(bestMarket.yes_bid),
        under_price: centsToAmerican(100 - (bestMarket.yes_ask || 0)),
        over_decimal: bestMarket.yes_bid ? (bestMarket.yes_bid / 100) : null,
        under_decimal: bestMarket.no_bid ? (bestMarket.no_bid / 100) : null,
        line_type: seriesConfig.stat.toLowerCase(),
        status: 'active',
        choice_display: null,
        player_name: event.sub_title || event.title,
        team_abbr: null,
        game_display: event.title,
        source: 'kalshi',
        updated_at: bestMarket.updated_time || new Date().toISOString()
      });
    }
  }

  return props;
}

// Main scrape function
async function scrapeKalshi() {
  const allProps = [];
  const errors = [];

  // Scrape player props
  for (const [ticker, config] of Object.entries(PLAYER_PROP_SERIES)) {
    try {
      const events = await fetchSeriesEvents(ticker);
      if (events.length > 0) {
        const props = processPlayerPropEvents(events, config);
        allProps.push(...props);
      }
      // Rate limit: small delay between series
      await new Promise(r => setTimeout(r, 200));
    } catch (err) {
      errors.push({ ticker, error: err.message });
    }
  }

  // Scrape game lines
  for (const [ticker, config] of Object.entries(GAME_LINE_SERIES)) {
    try {
      const events = await fetchSeriesEvents(ticker);
      if (events.length > 0) {
        const props = processGameLineEvents(events, config);
        allProps.push(...props);
      }
      await new Promise(r => setTimeout(r, 200));
    } catch (err) {
      errors.push({ ticker, error: err.message });
    }
  }

  return { props: allProps, errors };
}

module.exports = { scrapeKalshi, centsToAmerican };
