const WebSocket = require('ws');

const API_KEY = (process.env.AIS_API_KEY || '').trim();
const WORKER_URL = 'https://hamburg-schiffe.oliver-beer1989.workers.dev/update';
const UPLOAD_SECRET = process.env.UPLOAD_SECRET || 'aida-geheim-2026';
const GESAMT_ZEIT_MS = 210000;

function verbindeEinmal(restZeitMs, schiffe) {
  return new Promise((resolve) => {
    const ws = new WebSocket('wss://stream.aisstream.io/v0/stream');
    const timer = setTimeout(() => {
      try { ws.close(); } catch (e) {}
      resolve();
    }, restZeitMs);

    ws.on('open', () => {
      ws.send(JSON.stringify({
        APIKey: API_KEY,
        BoundingBoxes: [[[53.48, 8.65], [53.92, 10.12]]],
        FilterMessageTypes: ['ShipStaticData', 'PositionReport']
      }));
    });

    ws.on('message', (data) => {
      try {
        const msg = JSON.parse(data.toString());
        if (msg.MessageType === 'SubscriptionConfirmation') return;

        const mmsi = String(msg.MetaData?.MMSI || msg.Message?.PositionReport?.UserID || '');
        if (!mmsi) return;

        const eintrag = schiffe.get(mmsi) || {
          mmsi,
          name: '',
          laenge: 0,
          typ: 0,
          sog: null,
          cog: null,
          lat: null,
          lon: null,
          ziel: ''
        };

        if (msg.MetaData?.ShipName) eintrag.name = msg.MetaData.ShipName.trim();
        if (msg.MetaData?.latitude) eintrag.lat = msg.MetaData.latitude;
        if (msg.MetaData?.longitude) eintrag.lon = msg.MetaData.longitude;

        if (msg.Message?.PositionReport) {
          const p = msg.Message.PositionReport;
          eintrag.sog = p.Sog ?? eintrag.sog;
          eintrag.cog = p.Cog ?? eintrag.cog;
          if (p.Latitude) eintrag.lat = p.Latitude;
          if (p.Longitude) eintrag.lon = p.Longitude;
        }

        if (msg.Message?.ShipStaticData) {
          const s = msg.Message.ShipStaticData;
          if (s.Name) eintrag.name = s.Name.trim();
          if (s.Type) eintrag.typ = s.Type;
          if (s.Destination) eintrag.ziel = s.Destination.trim();
          if (s.Dimension) {
            eintrag.laenge = (s.Dimension.A || 0) + (s.Dimension.B || 0);
          }
        }

        schiffe.set(mmsi, eintrag);
      } catch (e) {}
    });

    ws.on('error', () => {});
    ws.on('close', () => {
      clearTimeout(timer);
      resolve();
    });
  });
}

async function run() {
  const schiffe = new Map();
  const ende = Date.now() + GESAMT_ZEIT_MS;

  while (Date.now() < ende) {
    const rest = ende - Date.now();
    if (rest <= 2000) break;
    await verbindeEinmal(rest, schiffe);
    if (Date.now() < ende) await new Promise((r) => setTimeout(r, 1000));
  }

  const treffer = [];
  const zeit = new Date().toLocaleTimeString('de-DE', {
    timeZone: 'Europe/Berlin',
    hour: '2-digit',
    minute: '2-digit'
  });

  for (const s of schiffe.values()) {
    // Schon ab 1.5 Knoten erfassen (wichtig beim Ablegen im Hafen)
    if (s.sog === null || s.sog < 1.5 || s.cog === null) continue;

    let richtung = '';
    const imHafenBereich = s.lon !== null && s.lon >= 9.85;

    // Einlaufend Richtung Hamburg (Ost / Südost)
    if (s.cog >= 65 && s.cog <= 170 && (s.lon === null || s.lon < 9.98)) {
      richtung = 'EINLAUFEND';
    } 
    // Auslaufend: Elbabwärts (225°-345°) ODER Ausfahrt aus Hafenbecken/Steinwerder nach Norden (345°-25°)
    else if ((s.cog >= 225 && s.cog <= 360) || (imHafenBereich && s.cog >= 0 && s.cog <= 25)) {
      richtung = 'AUSLAUFEND';
    }

    if (richtung) {
      treffer.push({
        mmsi: s.mmsi,
        name: s.name || `Schiff (${s.mmsi})`,
        laenge: s.laenge,
        typ: s.typ,
        sog: s.sog,
        lat: s.lat,
        lon: s.lon,
        richtung,
        ziel: s.ziel,
        zeit
      });
    }
  }

  console.log(`Empfangene Schiffe gesamt: ${schiffe.size}, davon in Bewegung/Richtung: ${treffer.length}`);

  const res = await fetch(WORKER_URL, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${UPLOAD_SECRET}`
    },
    body: JSON.stringify(treffer)
  });
  console.log('Worker-Antwort:', await res.text());
}

run();
