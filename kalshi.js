// Kalshi Scraper Module — Dynamic All-Sports Version
// Fetches all open sports markets from Kalshi API

const fetch = require('node-fetch');

const KALSHI_API = 'https://api.elections.kalshi.com/trade-api/v2';

// Series prefix → sport mapping for known series
const SPORT_MAP = {
  // Basketball
  'KXNBAPTS': 'NBA', 'KXNBAREB': 'NBA', 'KXNBAAST': 'NBA', 'KXNBA3PT': 'NBA',
  'KXNBAPRA': 'NBA', 'KXNBAPR': 'NBA', 'KXNBAPA': 'NBA', 'KXNBARA': 'NBA',
  'KXNBASTL': 'NBA', 'KXNBABLK': 'NBA', 'KXNBA2D': 'NBA', 'KXNBA3D': 'NBA',
  'KXNBAGAME': 'NBA', 'KXNBASPREAD': 'NBA', 'KXNBATOTAL': 'NBA',
  'KXNBATEAMTOTAL': 'NBA',
  'KXNBA1HSPREAD': 'NBA', 'KXNBA1HTOTAL': 'NBA', 'KXNBA2HSPREAD': 'NBA', 'KXNBA2HTOTAL': 'NBA',
  'KXNBA1QSPREAD': 'NBA', 'KXNBA1QTOTAL': 'NBA', 'KXNBA2QSPREAD': 'NBA', 'KXNBA2QTOTAL': 'NBA',
  'KXNBA3QSPREAD': 'NBA', 'KXNBA3QTOTAL': 'NBA', 'KXNBA4QSPREAD': 'NBA', 'KXNBA4QTOTAL': 'NBA',
  'KXNBA1HWINNER': 'NBA', 'KXNBA2HWINNER': 'NBA',
  'KXNBA1QWINNER': 'NBA', 'KXNBA2QWINNER': 'NBA', 'KXNBA3QWINNER': 'NBA', 'KXNBA4QWINNER': 'NBA',
  // All-Star
  'KXNBA3PTCONTEST': '3PT', 'KXNBASLAMDUNK': 'DUNK',
  'KXNBAALLSTARGAME': 'ASG', 'KXNBAALLSTARMVP': 'ASG', 'KXNBAALLSTAR': 'ASG',
  'KXNBACELEBRITY3PT': 'CELEB3PT', 'KXNBACELEBRITYGAME': 'CELEB',
  'KXNBARISINGSTARS': 'RISING', 'KXNBASHOOTINGSTARS': 'SHOOTING_STARS',
  // CBB
  'KXNCAAMBGAME': 'CBB', 'KXNCAAMBSPREAD': 'CBB', 'KXNCAAMBTOTAL': 'CBB',
  'KXNCAAWBGAME': 'WCBB', 'KXNCAAWBSPREAD': 'WCBB', 'KXNCAAWBTOTAL': 'WCBB',
  // NFL
  'KXNFLGAME': 'NFL', 'KXNFLSPREAD': 'NFL', 'KXNFLTOTAL': 'NFL',
  'KXNFLTEAMTOTAL': 'NFL', 'KXNFLANYTD': 'NFL', 'KXNFLFIRSTTD': 'NFL',
  'KXNFLPASSYDS': 'NFL', 'KXNFLRSHYDS': 'NFL', 'KXNFLRECYDS': 'NFL',
  'KXNFLPASSTDS': 'NFL', 'KXNFLREC': 'NFL',
  'KXNFL1HSPREAD': 'NFL', 'KXNFL1HTOTAL': 'NFL', 'KXNFL2HSPREAD': 'NFL', 'KXNFL2HTOTAL': 'NFL',
  'KXNFL1QSPREAD': 'NFL', 'KXNFL1QTOTAL': 'NFL', 'KXNFL2QSPREAD': 'NFL', 'KXNFL2QTOTAL': 'NFL',
  'KXNFL3QSPREAD': 'NFL', 'KXNFL3QTOTAL': 'NFL', 'KXNFL4QSPREAD': 'NFL', 'KXNFL4QTOTAL': 'NFL',
  'KXNCAAFGAME': 'NCAAF', 'KXNCAAFSPREAD': 'NCAAF', 'KXNCAAFTOTAL': 'NCAAF',
  'KXNCAAFTEAMTOTAL': 'NCAAF',
  // Tennis
  'KXATPGAME': 'TENNIS', 'KXATPMATCH': 'TENNIS', 'KXATPEXACTMATCH': 'TENNIS',
  'KXATPTOTALSETS': 'TENNIS', 'KXATPSETWINNER': 'TENNIS', 'KXATPANYSET': 'TENNIS',
  'KXATPDOUBLES': 'TENNIS', 'KXATPCHALLENGERMATCH': 'TENNIS',
  'KXWTAGAME': 'TENNIS', 'KXWTAMATCH': 'TENNIS', 'KXWTADOUBLES': 'TENNIS',
  'KXWTACHALLENGERMATCH': 'TENNIS',
  // Golf
  'KXPGATOUR': 'PGA', 'KXPGATOP5': 'PGA', 'KXPGATOP10': 'PGA', 'KXPGATOP20': 'PGA',
  'KXPGAMAKECUT': 'PGA', 'KXPGAR1LEAD': 'PGA',
  'KXLIVTOUR': 'PGA', 'KXLIVTOP5': 'PGA', 'KXLIVTOP10': 'PGA', 'KXLIVR1LEAD': 'PGA',
  'KXDPWORLDTOUR': 'PGA', 'KXGENESISINVITATIONAL': 'PGA', 'KXPHOENIXOPEN': 'PGA',
  'KXPGARYDERMATCH': 'PGA',
  // Baseball
  'KXMLBGAME': 'MLB', 'KXMLBSPREAD': 'MLB', 'KXMLBTOTAL': 'MLB',
  'KXMLBRFI': 'MLB', 'KXMLBSERIES': 'MLB', 'KXMLBSERIESEXACT': 'MLB',
  'KXMLBSERIESGAMETOTAL': 'MLB', 'KXMLBASGAME': 'MLB',
  // MMA
  'KXUFCFIGHT': 'MMA', 'KXUFCMOV': 'MMA', 'KXUFCROUNDS': 'MMA',
  'KXUFCDISTANCE': 'MMA', 'KXUFCVICROUND': 'MMA', 'KXUFCMOF': 'MMA',
  // NHL
  'KXNHLGAME': 'NHL', 'KXNHLSPREAD': 'NHL', 'KXNHLTOTAL': 'NHL',
  'KXNHLGOAL': 'NHL', 'KXNHLAST': 'NHL', 'KXNHLPTS': 'NHL', 'KXNHLSAVES': 'NHL',
  'KXNHLFIRSTGOAL': 'NHL', 'KXNHLANYGOAL': 'NHL',
  // Olympics
  'KXWO': 'OLYMPICS', 'KXWOFSKATE': 'OLYMPICS', 'KXWOLUGE': 'OLYMPICS',
  'KXWOSBOARD': 'OLYMPICS', 'KXWOFREESKI': 'OLYMPICS', 'KXWOBIATH': 'OLYMPICS',
  'KXWOCURL': 'OLYMPICS', 'KXWOCURLGAME': 'OLYMPICS', 'KXWOSSKATE': 'OLYMPICS',
  'KXWOSTRACK': 'OLYMPICS', 'KXWOHOCKEY': 'OLYMPICS', 'KXWOMHOCKEY': 'OLYMPICS',
  'KXWOWHOCKEY': 'OLYMPICS', 'KXWOMHOCKEYSPREAD': 'OLYMPICS', 'KXWOMHOCKEYTOTAL': 'OLYMPICS',
  'KXWOSKIJUMP': 'OLYMPICS', 'KXWOXC': 'OLYMPICS', 'KXWONORDIC': 'OLYMPICS',
  'KXWOSKEL': 'OLYMPICS', 'KXWOALPSKI': 'OLYMPICS', 'KXWOSKIMTN': 'OLYMPICS',
};

// Prefix-based fallback for series not in the map
function guessSport(ticker) {
  if (SPORT_MAP[ticker]) return SPORT_MAP[ticker];
  if (ticker.startsWith('KXNBA')) return 'NBA';
  if (ticker.startsWith('KXNCAAMB')) return 'CBB';
  if (ticker.startsWith('KXNCAAWB')) return 'WCBB';
  if (ticker.startsWith('KXNCAAF')) return 'NCAAF';
  if (ticker.startsWith('KXNFL')) return 'NFL';
  if (ticker.startsWith('KXMLB')) return 'MLB';
  if (ticker.startsWith('KXNHL')) return 'NHL';
  if (ticker.startsWith('KXUFC')) return 'MMA';
  if (ticker.startsWith('KXATP') || ticker.startsWith('KXWTA')) return 'TENNIS';
  if (ticker.startsWith('KXPGA') || ticker.startsWith('KXLIV') || ticker.startsWith('KXDPWORLD')) return 'PGA';
  if (ticker.startsWith('KXWO')) return 'OLYMPICS';
  return null; // Unknown — skip
}

// Stat type inference from series ticker
function inferStatType(ticker, title) {
  const tk = ticker.toUpperCase();
  if (tk.includes('PTS') && !tk.includes('3PT')) return 'Points';
  if (tk.includes('REB')) return 'Rebounds';
  if (tk.includes('AST') && !tk.includes('MASTER')) return 'Assists';
  if (tk.endsWith('3PT') && !tk.includes('CONTEST') && !tk.includes('CELEBRITY')) return '3-Pointers Made';
  if (tk.includes('PRA')) return 'Pts + Rebs + Asts';
  if (tk.endsWith('PR') || tk.includes('NBAPR')) return 'Points + Rebounds';
  if (tk.endsWith('PA') || tk.includes('NBAPA')) return 'Points + Assists';
  if (tk.endsWith('RA') || tk.includes('NBARA')) return 'Rebounds + Assists';
  if (tk.includes('STL')) return 'Steals';
  if (tk.includes('BLK')) return 'Blocks';
  if (tk.includes('PASSYDS')) return 'Passing Yards';
  if (tk.includes('RSHYDS') || tk.includes('RUSHYDS')) return 'Rushing Yards';
  if (tk.includes('RECYDS')) return 'Receiving Yards';
  if (tk.includes('PASSTDS')) return 'Passing TDs';
  if (tk.endsWith('REC') && !tk.includes('RECORD')) return 'Receptions';
  if (tk.includes('ANYTD')) return 'Anytime TD';
  if (tk.includes('FIRSTTD') && !tk.includes('TIME')) return 'First TD';
  if (tk.includes('SAVES')) return 'Saves';
  if (tk.includes('GOAL') && !tk.includes('TOTAL')) return 'Goals';
  if (tk.includes('GAME') && !tk.includes('TOTAL') && !tk.includes('SACK') && !tk.includes('FG') && !tk.includes('TD') && !tk.includes('TO')) return 'Moneyline';
  if (tk.includes('SPREAD')) return 'Spread';
  if (tk.includes('TOTAL')) return 'Total';
  if (tk.includes('MATCH')) return 'Match Winner';
  if (tk.includes('FIGHT')) return 'Fight Winner';
  if (tk.includes('CONTEST')) return title.includes('3-Point') ? '3PT Contest Winner' : 'Contest Winner';
  if (tk.includes('SLAMDUNK')) return 'Dunk Contest Winner';
  if (tk.includes('MVP')) return 'MVP';
  if (tk.includes('TOP5')) return 'Top 5';
  if (tk.includes('TOP10')) return 'Top 10';
  if (tk.includes('TOP20')) return 'Top 20';
  if (tk.includes('MAKECUT')) return 'Make Cut';
  if (tk.includes('R1LEAD')) return 'R1 Leader';
  if (tk.includes('DISTANCE')) return 'Go The Distance';
  if (tk.includes('MOV') || tk.includes('MOF')) return 'Method of Victory';
  if (tk.includes('ROUNDS')) return 'Total Rounds';
  if (tk.includes('VICROUND')) return 'Victory Round';
  return title || ticker;
}

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
function extractPlayerName(title, subtitle) {
  if (subtitle) {
    // Clean up subtitle like ":: Philadelphia"
    const clean = subtitle.replace(/^::\s*/, '').trim();
    if (clean && clean.length > 2) return clean;
  }
  const match = title?.match(/^Will (.+?) (?:score|get|have|record|win|make|go|finish)/i);
  if (match) return match[1].trim();
  return title || 'Unknown';
}

// Extract line value from ticker
function extractLineFromTicker(ticker) {
  // e.g. KXNBAPTS-26FEB12DALAL-LBJ-O25 -> 25
  const parts = ticker.split('-');
  for (const part of parts) {
    const match = part.match(/^[OU](\d+\.?\d*)$/);
    if (match) return parseFloat(match[1]);
  }
  return null;
}

async function fetchWithRetry(url, retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      const resp = await fetch(url, { timeout: 30000 });
      if (resp.status === 429) {
        await new Promise(r => setTimeout(r, 2000 * (i + 1)));
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

// Fetch all open events for a series (with nested markets)
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

// Discover all open sports series
async function discoverSportsSeries() {
  const allSeries = [];
  let cursor = null;
  do {
    const url = `${KALSHI_API}/series?limit=200${cursor ? `&cursor=${cursor}` : ''}`;
    const data = await fetchWithRetry(url);
    const series = data.series || [];
    allSeries.push(...series);
    cursor = series.length === 200 ? data.cursor : null;
  } while (cursor);

  // Filter to series we can map to a sport
  return allSeries.filter(s => guessSport(s.ticker) !== null);
}

// Process events into props
function processEvents(events, seriesTicker, sport, statType) {
  const props = [];

  for (const event of events) {
    const markets = event.markets || [];
    if (markets.length === 0) continue;

    // Check if this is a player prop (multiple strikes per event = over/under)
    // vs a winner market (multiple players per event = binary each)
    const isOverUnder = markets.some(m => m.ticker?.match(/[OU]\d/));
    const isWinnerMarket = !isOverUnder && markets.length > 1 && markets.every(m => m.yes_bid != null);

    if (isOverUnder) {
      // Player prop — find best market (closest to 50/50)
      const openMarkets = markets.filter(m => m.status === 'open' || m.status === 'active');
      if (openMarkets.length === 0) continue;

      const bestMarket = openMarkets.reduce((best, m) => {
        const dist = Math.abs((m.yes_bid || 50) - 50);
        const bestDist = Math.abs((best.yes_bid || 50) - 50);
        return dist < bestDist ? m : best;
      }, openMarkets[0]);

      const lineValue = extractLineFromTicker(bestMarket.ticker) || bestMarket.floor_strike || bestMarket.cap_strike;
      if (lineValue == null) continue;

      const playerName = extractPlayerName(event.title, event.sub_title);

      props.push({
        id: `kalshi_${bestMarket.ticker}`,
        appearance_id: null,
        player_id: null,
        game_id: event.event_ticker,
        sport_id: sport,
        stat_type: statType,
        stat_display: statType,
        stat_value: lineValue,
        over_price: centsToAmerican(bestMarket.yes_bid),
        under_price: centsToAmerican(100 - (bestMarket.yes_ask || 0)),
        over_decimal: bestMarket.yes_bid ? (bestMarket.yes_bid / 100) : null,
        under_decimal: bestMarket.no_bid ? (bestMarket.no_bid / 100) : null,
        line_type: 'over_under',
        status: 'active',
        choice_display: null,
        player_name: playerName,
        team_abbr: null,
        game_display: event.title,
        source: 'kalshi',
        updated_at: bestMarket.updated_time || new Date().toISOString()
      });
    } else if (isWinnerMarket || markets.length >= 1) {
      // Winner/binary market OR single game line
      for (const market of markets) {
        if (market.status !== 'open' && market.status !== 'active') continue;

        let playerName = 'Unknown';
        const nameMatch = market.title?.match(/^Will (.+?) (?:win|score|get|have|make|go|finish|be)/i);
        if (nameMatch) {
          playerName = nameMatch[1].trim();
        } else if (market.subtitle) {
          playerName = market.subtitle.replace(/^::\s*/, '').trim();
        } else {
          playerName = event.sub_title || event.title;
        }

        const yesBid = market.yes_bid || 0;
        const yesAsk = market.yes_ask || 0;

        // For game lines with spreads/totals, extract line from ticker
        const lineFromTicker = extractLineFromTicker(market.ticker);

        props.push({
          id: `kalshi_${market.ticker}`,
          appearance_id: null,
          player_id: null,
          game_id: event.event_ticker,
          sport_id: sport,
          stat_type: statType,
          stat_display: statType,
          stat_value: lineFromTicker || yesBid,
          over_price: centsToAmerican(yesBid),
          under_price: centsToAmerican(100 - yesAsk),
          over_decimal: yesBid ? (yesBid / 100) : null,
          under_decimal: (100 - yesAsk) > 0 ? ((100 - yesAsk) / 100) : null,
          line_type: isWinnerMarket ? 'winner' : (lineFromTicker ? 'game_line' : 'binary'),
          status: 'active',
          choice_display: isWinnerMarket ? `${yesBid}¢ yes` : null,
          player_name: playerName,
          team_abbr: market.subtitle?.replace(/^::\s*/, '').trim() || null,
          game_display: event.title || statType,
          source: 'kalshi',
          updated_at: market.updated_time || new Date().toISOString()
        });
      }
    }
  }

  return props;
}

// Main scrape function — fetches ALL open events in bulk, then filters
async function scrapeKalshi() {
  const allProps = [];
  const errors = [];

  try {
    // Step 1: Fetch ALL open events with nested markets (paginated)
    const allEvents = [];
    let cursor = null;
    let page = 0;
    do {
      const url = `${KALSHI_API}/events?limit=200&status=open&with_nested_markets=true${cursor ? `&cursor=${cursor}` : ''}`;
      const data = await fetchWithRetry(url);
      const events = data.events || [];
      allEvents.push(...events);
      cursor = events.length === 200 ? data.cursor : null;
      page++;
    } while (cursor && page < 50);

    console.log(`[KALSHI] Fetched ${allEvents.length} open events (${page} pages)`);

    // Step 2: Filter to sports we can map and process
    let matched = 0;
    let skipped = 0;

    for (const event of allEvents) {
      const sport = guessSport(event.series_ticker);
      if (!sport) { skipped++; continue; }

      const statType = inferStatType(event.series_ticker, event.title);
      try {
        const props = processEvents([event], event.series_ticker, sport, statType);
        allProps.push(...props);
        if (props.length > 0) matched++;
      } catch (err) {
        errors.push({ ticker: event.series_ticker, error: err.message });
      }
    }

    console.log(`[KALSHI] Processed ${matched} events → ${allProps.length} props (skipped ${skipped} non-sports, ${errors.length} errors)`);
  } catch (err) {
    errors.push({ ticker: 'bulk_fetch', error: err.message });
    console.error(`[KALSHI] Bulk fetch failed:`, err.message);
  }

  return { props: allProps, errors };
}

module.exports = { scrapeKalshi };
