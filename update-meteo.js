import { createClient } from '@supabase/supabase-js';
import fetch from 'node-fetch';

// Configuration Supabase
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

// Vérifier que les variables d'environnement sont définies
if (!supabaseUrl || !supabaseKey) {
  console.error("Variables d'environnement manquantes");
  console.error("SUPABASE_URL:", supabaseUrl ? "Définie" : "Non définie");
  console.error("SUPABASE_SERVICE_ROLE_KEY:", supabaseKey ? "Définie" : "Non définie");
  process.exit(1);
}

const supabase = createClient(supabaseUrl, supabaseKey);

// Paramètres de performance
const CONCURRENT_BATCH_SIZE = 20; // Nombre d'aérodromes traités simultanément
const RETRY_ATTEMPTS = 3; // Nombre de tentatives en cas d'échec
const RETRY_DELAY = 500; // Délai entre les tentatives (ms)

// Fonction avec retry pour les appels HTTP
async function fetchWithRetry(url, retries = RETRY_ATTEMPTS) {
  try {
    const response = await fetch(url);
    return response;
  } catch (error) {
    if (retries <= 1) throw error;
    await new Promise(resolve => setTimeout(resolve, RETRY_DELAY));
    return fetchWithRetry(url, retries - 1);
  }
}

// Fonction pour analyser en profondeur un METAR
function parseMetarDetails(rawMetar) {
  // Initialisation des résultats
  const result = {
    ventDirection: null,
    ventVariable: false,
    ventVitesse: null,
    ventRafales: null,
    ventUnites: 'KT',
    visibilite: null,
    phenomenes: [],
    nuages: [],
    temperature: null,
    pointRosee: null,
    qnh: null
  };
  
  // Diviser le METAR en parties
  const parts = rawMetar.split(' ');
  
  // Ignorer le code OACI et la date/heure (premiers éléments)
  let i = 2; // Commencer après le code OACI et la date/heure
  
  // Variables pour le traitement spécial
  let ventVariation = null;
  
  while (i < parts.length) {
    const part = parts[i];
    
    // 1. Analyse du vent (tous les formats possibles)
    if (/^(\d{3}|VRB)(\d{2,3})(G\d{2,3})?(KT|MPS)$/.test(part) || 
        /^\/\/\/(\d{2,3})(G\d{2,3})?(KT|MPS)$/.test(part) || 
        /^00000(KT|MPS)$/.test(part)) {
      
      if (part.startsWith('VRB')) {
        // Format: VRBffGffKT - Vent de direction variable
        result.ventVariable = true;
        result.ventDirection = null;
        
        const match = part.match(/VRB(\d{2,3})(G(\d{2,3}))?(KT|MPS)/);
        if (match) {
          result.ventVitesse = parseInt(match[1]);
          if (match[3]) result.ventRafales = parseInt(match[3]);
          result.ventUnites = match[4];
        }
      } else if (part.startsWith('///')) {
        // Format: ///ffGffKT - Direction inconnue
        result.ventDirection = null;
        
        const match = part.match(/\/\/\/(\d{2,3})(G(\d{2,3}))?(KT|MPS)/);
        if (match) {
          result.ventVitesse = parseInt(match[1]);
          if (match[3]) result.ventRafales = parseInt(match[3]);
          result.ventUnites = match[4];
        }
      } else if (part === '00000KT' || part === '00000MPS') {
        // Calme (pas de vent)
        result.ventDirection = 0;
        result.ventVitesse = 0;
        result.ventUnites = part.endsWith('KT') ? 'KT' : 'MPS';
      } else {
        // Format standard: dddffGffKT
        const match = part.match(/^(\d{3})(\d{2,3})(G(\d{2,3}))?(KT|MPS)$/);
        if (match) {
          result.ventDirection = parseInt(match[1]);
          result.ventVitesse = parseInt(match[2]);
          if (match[4]) result.ventRafales = parseInt(match[4]);
          result.ventUnites = match[5];
        }
      }
    }
    // 2. Variation de la direction du vent (format: dddVddd)
    else if (/^\d{3}V\d{3}$/.test(part)) {
      ventVariation = part; // Par exemple "240V300"
    }
    // 3. Analyse de la visibilité
    else if (/^\d{4}$/.test(part) || part === '9999') {
      result.visibilite = parseInt(part);
    }
    else if (part === 'CAVOK') {
      result.visibilite = 9999; // Plus de 10km
    }
    // 4. Analyse des phénomènes météo
    else if (/^(\+|-|VC|RE)?([A-Z]{2,})$/.test(part) && 
             !part.startsWith('Q') && 
             !part.startsWith('RMK')) {
      result.phenomenes.push(part);
    }
    // 5. Analyse des nuages
    else if (/^(FEW|SCT|BKN|OVC)(\d{3})(CB|TCU)?$/.test(part)) {
      const match = part.match(/^(FEW|SCT|BKN|OVC)(\d{3})(CB|TCU)?$/);
      if (match) {
        result.nuages.push({
          type: match[1],
          hauteur: parseInt(match[2]) * 100, // hauteur en pieds
          orage: match[3] === 'CB' || match[3] === 'TCU'
        });
      }
    }
    // 6. Analyse de la température et du point de rosée
    else if (/^(M)?(\d{1,2})\/(M)?(\d{1,2})$/.test(part)) {
      const match = part.match(/^(M)?(\d{1,2})\/(M)?(\d{1,2})$/);
      if (match) {
        result.temperature = (match[1] ? -1 : 1) * parseInt(match[2]);
        result.pointRosee = (match[3] ? -1 : 1) * parseInt(match[4]);
      }
    }
    // 7. Analyse de la pression QNH
    else if (/^Q\d{4}$/.test(part)) {
      result.qnh = parseInt(part.substring(1));
    }
    else if (/^A\d{4}$/.test(part)) {
      // Conversion de pouces de mercure en hPa
      const inchesHg = parseInt(part.substring(1)) / 100;
      result.qnh = Math.round(inchesHg * 33.8639);
    }
    
    i++;
  }
  
  return result;
}

// Fonction pour mettre à jour un METAR
async function updateMetar(icaoCode) {
  const url = `https://tgftp.nws.noaa.gov/data/observations/metar/stations/${icaoCode}.TXT`;
  
  try {
    const response = await fetchWithRetry(url);
    if (!response.ok) return false;
    
    const text = await response.text();
    const lines = text.trim().split('\n');
    
    if (lines.length < 2) return false;
    
    const dateStr = lines[0].trim();
    const rawMetar = lines[1].trim();
    const observationDate = new Date(dateStr);
    
    // Analyser le METAR avec notre nouvelle fonction détaillée
    const parsedMetar = parseMetarDetails(rawMetar);
    
    // Insérer dans Supabase avec toutes les informations analysées
    const { error } = await supabase
      .from('metars')
      .upsert({
        code_oaci: icaoCode,
        raw_metar: rawMetar,
        date_observation: observationDate.toISOString(),
        vent_direction: parsedMetar.ventDirection,
        vent_variable: parsedMetar.ventVariable, 
        vent_vitesse: parsedMetar.ventVitesse,
        vent_rafales: parsedMetar.ventRafales,
        vent_unites: parsedMetar.ventUnites,
        visibilite: parsedMetar.visibilite,
        phenomenes: parsedMetar.phenomenes,
        nuages: parsedMetar.nuages,
        temperature: parsedMetar.temperature,
        point_rosee: parsedMetar.pointRosee,
        qnh: parsedMetar.qnh,
        updated_at: new Date().toISOString()
      }, { onConflict: 'code_oaci' });
    
    if (error) throw error;
    
    return true;
  } catch (error) {
    console.error(`Erreur METAR ${icaoCode}:`, error.message);
    return false;
  }
}

// Fonction améliorée pour analyser les segments TAF
async function parseTafSegments(tafData, icaoCode) {
  try {
    const { raw_taf, validite_debut, validite_fin, id, date_emission } = tafData;
    
    // Supprimer les segments existants pour ce TAF
    await supabase
      .from('taf_segments')
      .delete()
      .eq('taf_id', id);
    
    const tafText = raw_taf.trim();
    
    // Créer une structure pour l'analyse des segments
    const segments = [];
    
    // Extraire le jour du mois de la date d'émission pour calculer les dates
    const emissionDate = new Date(date_emission);
    const emissionDay = emissionDate.getUTCDate();
    const emissionMonth = emissionDate.getUTCMonth();
    const emissionYear = emissionDate.getUTCFullYear();
    
    // Extraire la période de validité globale une fois de plus pour l'analyser
    const validityPattern = /\b(\d{2})(\d{2})\/(\d{2})(\d{2})\b/;
    const validityMatch = tafText.match(validityPattern);
    let tafStartDay, tafStartHour, tafEndDay, tafEndHour;
    
    if (validityMatch) {
      tafStartDay = parseInt(validityMatch[1]);
      tafStartHour = parseInt(validityMatch[2]);
      tafEndDay = parseInt(validityMatch[3]);
      tafEndHour = parseInt(validityMatch[4]);
    }
    
    // Découper le TAF en tokens (mots)
    const tokens = tafText.split(' ');
    
    // Ignorer l'en-tête (code OACI, date d'émission, période de validité)
    let currentIndex = 3; // Position typique après l'en-tête
    while (currentIndex < tokens.length && 
          (tokens[currentIndex].match(/^\d{2}\d{2}\/\d{2}\d{2}$/) || 
           tokens[currentIndex] === 'AMD' || 
           tokens[currentIndex] === 'COR')) {
      currentIndex++;
    }
    
    // Le segment initial commence après l'en-tête
    let currentSegment = {
      type: 'INIT',
      startIndex: currentIndex,
      text: [],
      valid_from: validite_debut,
      valid_to: validite_fin,
      probability: null
    };
    
    // Parcourir le reste du TAF
    for (let i = currentIndex; i < tokens.length; i++) {
      const token = tokens[i];
      
      // Détecter les marqueurs de changement
      if (token === 'BECMG' || token === 'TEMPO') {
        // Terminer le segment courant
        currentSegment.text = tokens.slice(currentSegment.startIndex, i);
        segments.push(currentSegment);
        
        // Chercher la période du changement (format: ddHH/ddHH)
        const timeGroup = tokens[i+1].match(/^(\d{2})(\d{2})\/(\d{2})(\d{2})$/);
        let changeStart = null;
        let changeEnd = null;
        
        if (timeGroup) {
          const fromDay = parseInt(timeGroup[1]);
          const fromHour = parseInt(timeGroup[2]);
          const toDay = parseInt(timeGroup[3]);
          const toHour = parseInt(timeGroup[4]);
          
          // Créer des dates complètes
          changeStart = new Date(Date.UTC(emissionYear, emissionMonth, fromDay, fromHour, 0, 0));
          changeEnd = new Date(Date.UTC(emissionYear, emissionMonth, toDay, toHour, 0, 0));
          
          // Ajuster si passage de mois
          if (fromDay < emissionDay && emissionDay - fromDay > 20) {
            changeStart.setUTCMonth(changeStart.getUTCMonth() + 1);
          }
          if (toDay < fromDay && fromDay - toDay > 20) {
            changeEnd.setUTCMonth(changeEnd.getUTCMonth() + 1);
          }
          
          // Avancer l'index pour sauter le groupe temporel
          i++;
        }
        
        // Créer un nouveau segment
        currentSegment = {
          type: token,
          startIndex: i + 1,
          text: [],
          valid_from: changeStart || validite_debut,
          valid_to: changeEnd || validite_fin,
          probability: null
        };
      }
      else if (token.startsWith('PROB')) {
        // Terminer le segment courant
        currentSegment.text = tokens.slice(currentSegment.startIndex, i);
        segments.push(currentSegment);
        
        // Extraire la probabilité
        const probability = parseInt(token.substring(4));
        
        // Chercher la période du changement (format: ddHH/ddHH)
        const timeGroup = tokens[i+1].match(/^(\d{2})(\d{2})\/(\d{2})(\d{2})$/);
        let probStart = null;
        let probEnd = null;
        
        if (timeGroup) {
          const fromDay = parseInt(timeGroup[1]);
          const fromHour = parseInt(timeGroup[2]);
          const toDay = parseInt(timeGroup[3]);
          const toHour = parseInt(timeGroup[4]);
          
          // Créer des dates complètes
          probStart = new Date(Date.UTC(emissionYear, emissionMonth, fromDay, fromHour, 0, 0));
          probEnd = new Date(Date.UTC(emissionYear, emissionMonth, toDay, toHour, 0, 0));
          
          // Ajuster si passage de mois
          if (fromDay < emissionDay && emissionDay - fromDay > 20) {
            probStart.setUTCMonth(probStart.getUTCMonth() + 1);
          }
          if (toDay < fromDay && fromDay - toDay > 20) {
            probEnd.setUTCMonth(probEnd.getUTCMonth() + 1);
          }
          
          // Avancer l'index pour sauter le groupe temporel
          i++;
        }
        
        // Créer un nouveau segment
        currentSegment = {
          type: 'PROB',
          startIndex: i + 1,
          text: [],
          valid_from: probStart || validite_debut,
          valid_to: probEnd || validite_fin,
          probability: probability
        };
      }
      else if (token.startsWith('FM')) {
        // Terminer le segment courant
        currentSegment.text = tokens.slice(currentSegment.startIndex, i);
        segments.push(currentSegment);
        
        // Extraire l'heure de début (format: FMddHHmm)
        const fmMatch = token.match(/^FM(\d{2})(\d{2})(\d{2})$/);
        let fmStart = null;
        
        if (fmMatch) {
          const fmDay = parseInt(fmMatch[1]);
          const fmHour = parseInt(fmMatch[2]);
          const fmMinute = parseInt(fmMatch[3]);
          
          // Créer une date complète
          fmStart = new Date(Date.UTC(emissionYear, emissionMonth, fmDay, fmHour, fmMinute, 0));
          
          // Ajuster si passage de mois
          if (fmDay < emissionDay && emissionDay - fmDay > 20) {
            fmStart.setUTCMonth(fmStart.getUTCMonth() + 1);
          }
        }
        
        // Créer un nouveau segment
        currentSegment = {
          type: 'FM',
          startIndex: i + 1,
          text: [],
          valid_from: fmStart || validite_debut,
          valid_to: validite_fin,
          probability: null
        };
      }
    }
    
    // Ajouter le dernier segment
    currentSegment.text = tokens.slice(currentSegment.startIndex);
    segments.push(currentSegment);
    
    // Traiter chaque segment pour extraire les informations météo
    for (const segment of segments) {
      // Ne traiter que les segments qui ont du texte
      if (segment.text.length === 0) continue;
      
      // Reconstruire le texte du segment
      const segmentText = segment.text.join(' ');
      
      // Initialiser les données météo
      let ventDirection = null;
      let ventVariable = false;
      let ventVitesse = null;
      let ventRafales = null;
      let ventUnites = 'KT';
      let visibilite = null;
      let nuages = [];
      let phenomenes = [];
      
      // Analyser chaque token du segment
      for (const token of segment.text) {
        // Analyse du vent (tous formats possibles)
        if (/^(\d{3}|VRB)(\d{2,3})(G\d{2,3})?(KT|MPS)$/.test(token) || 
            /^\/\/\/(\d{2,3})(G\d{2,3})?(KT|MPS)$/.test(token) || 
            /^00000(KT|MPS)$/.test(token)) {
          
          if (token.startsWith('VRB')) {
            // Format: VRBffGffKT - Vent de direction variable
            ventVariable = true;
            ventDirection = null;
            
            const windMatch = token.match(/VRB(\d{2,3})(G(\d{2,3}))?(KT|MPS)/);
            if (windMatch) {
              ventVitesse = parseInt(windMatch[1]);
              if (windMatch[3]) ventRafales = parseInt(windMatch[3]);
              ventUnites = windMatch[4];
            }
          } else if (token.startsWith('///')) {
            // Format: ///ffGffKT - Direction inconnue
            ventDirection = null;
            ventVariable = false;
            
            const windMatch = token.match(/\/\/\/(\d{2,3})(G(\d{2,3}))?(KT|MPS)/);
            if (windMatch) {
              ventVitesse = parseInt(windMatch[1]);
              if (windMatch[3]) ventRafales = parseInt(windMatch[3]);
              ventUnites = windMatch[4];
            }
          } else if (token === '00000KT' || token === '00000MPS') {
            // Calme (pas de vent)
            ventDirection = 0;
            ventVitesse = 0;
            ventVariable = false;
            ventUnites = token.endsWith('KT') ? 'KT' : 'MPS';
          } else {
            // Format standard: dddffGffKT
            const windMatch = token.match(/^(\d{3})(\d{2,3})(G(\d{2,3}))?(KT|MPS)$/);
            if (windMatch) {
              ventDirection = parseInt(windMatch[1]);
              ventVitesse = parseInt(windMatch[2]);
              ventVariable = false;
              if (windMatch[4]) ventRafales = parseInt(windMatch[4]);
              ventUnites = windMatch[5];
            }
          }
          continue;
        }
        
        // Analyse de la visibilité
        if (/^\d{4}$/.test(token) && parseInt(token) <= 9999) {
          visibilite = parseInt(token);
          continue;
        }
        
        if (token === 'CAVOK' || token === '9999') {
          visibilite = 9999; // Plus de 10km
          continue;
        }
        
        // Analyse des nuages
        if (/^(FEW|SCT|BKN|OVC|NSC|NCD|CLR|SKC)(\d{3})?(CB|TCU)?$/.test(token)) {
          if (token === 'NSC' || token === 'NCD' || token === 'CLR' || token === 'SKC') {
            // Pas de nuages significatifs ou ciel clair
            nuages.push({
              type: token,
              hauteur: null,
              orage: false
            });
          } else {
            const cloudMatch = token.match(/^(FEW|SCT|BKN|OVC)(\d{3})(CB|TCU)?$/);
            if (cloudMatch) {
              nuages.push({
                type: cloudMatch[1],
                hauteur: parseInt(cloudMatch[2]) * 100, // hauteur en pieds
                orage: cloudMatch[3] === 'CB' || cloudMatch[3] === 'TCU'
              });
            }
          }
          continue;
        }
        
        // Analyse des phénomènes météo
        const wxPattern = /^(\+|-|VC)?([A-Z]{2,})$/;
        if (wxPattern.test(token) && 
           !token.startsWith('Q') && 
           !token.startsWith('A') && // QNH en pouces de mercure
           !token.startsWith('RMK') &&
           !['BECMG', 'TEMPO', 'PROB30', 'PROB40', 'FM', 'AMD', 'COR', 'CNL', 'NIL'].includes(token)) {
          
          const wxMatch = token.match(wxPattern);
          if (wxMatch) {
            const intensity = wxMatch[1] || null; // +, -, VC ou null
            const phenomenon = wxMatch[2];
            
            // Déterminer le type de phénomène
            let type = '';
            if (['DZ', 'RA', 'SN', 'SG', 'IC', 'PL', 'GR', 'GS', 'UP'].includes(phenomenon)) {
              type = 'precipitation';
            } else if (['BR', 'FG', 'FU', 'VA', 'DU', 'SA', 'HZ', 'PY'].includes(phenomenon)) {
              type = 'obscuration';
            } else if (['PO', 'SQ', 'FC', 'SS', 'DS'].includes(phenomenon)) {
              type = 'other';
            } else if (['TS', 'SH'].includes(phenomenon)) {
              type = 'vicinity';
            }
            
            phenomenes.push({
              code: token,
              type: type,
              intensity: intensity
            });
          }
        }
      }
      
      // Insérer le segment traité dans la base de données
      await supabase.from('taf_segments').insert({
        taf_id: id,
        code_oaci: icaoCode,
        segment_type: segment.type,
        probability: segment.probability,
        valide_debut: segment.valid_from,
        valide_fin: segment.valid_to,
        raw_segment: segmentText,
        vent_direction: ventDirection,
        vent_variable: ventVariable,
        vent_vitesse: ventVitesse,
        vent_rafales: ventRafales,
        vent_unites: ventUnites,
        visibilite: visibilite,
        nuages: nuages.length > 0 ? nuages : null,
        phenomenes: phenomenes.length > 0 ? phenomenes : null
      });
    }
    
    return segments.length;
  } catch (error) {
    console.error(`Erreur lors de l'analyse des segments TAF pour ${icaoCode}:`, error.message);
    return 0;
  }
}

// Fonction pour mettre à jour le TAF
async function updateTaf(icaoCode) {
  const url = `https://tgftp.nws.noaa.gov/data/forecasts/taf/stations/${icaoCode}.TXT`;
  
  try {
    const response = await fetchWithRetry(url);
    if (!response.ok) return false;
    
    const text = await response.text();
    const lines = text.trim().split('\n');
    
    if (lines.length < 2) return false;
    
    const dateStr = lines[0].trim();
    const rawTaf = lines.slice(1).join(' ').trim();
    const emissionDate = new Date(dateStr);
    
    // Extraire la période de validité (format: 0212/0318)
    const validityPattern = /\b(\d{2})(\d{2})\/(\d{2})(\d{2})\b/;
    const validityMatch = rawTaf.match(validityPattern);
    let validiteDebut = null;
    let validiteFin = null;
    
    if (validityMatch) {
      const day1 = parseInt(validityMatch[1]);
      const hour1 = parseInt(validityMatch[2]);
      const day2 = parseInt(validityMatch[3]);
      const hour2 = parseInt(validityMatch[4]);
      
      // Calculer les dates complètes
      const now = new Date();
      const month = now.getUTCMonth();
      const year = now.getUTCFullYear();
      
      validiteDebut = new Date(Date.UTC(year, month, day1, hour1, 0, 0));
      validiteFin = new Date(Date.UTC(year, month, day2, hour2, 0, 0));
      
      // Ajuster si la fin est avant le début (passage de mois)
      if (validiteFin < validiteDebut) {
        validiteFin.setUTCMonth(validiteFin.getUTCMonth() + 1);
      }
    }
    
    // Insérer/mettre à jour le TAF principal
    const { data, error } = await supabase
      .from('tafs')
      .upsert({
        code_oaci: icaoCode,
        raw_taf: rawTaf,
        date_emission: emissionDate.toISOString(),
        validite_debut: validiteDebut ? validiteDebut.toISOString() : null,
        validite_fin: validiteFin ? validiteFin.toISOString() : null,
        updated_at: new Date().toISOString()
      }, { 
        onConflict: 'code_oaci',
        returning: 'representation' // Pour récupérer l'ID et d'autres champs
      });
    
    if (error) throw error;
    
    // Analyser et insérer les segments du TAF
    if (data && data.length > 0) {
      const tafData = data[0];
      const segmentsCount = await parseTafSegments(tafData, icaoCode);
      console.log(`TAF pour ${icaoCode} découpé en ${segmentsCount} segments`);
    }
    
    return true;
  } catch (error) {
    console.error(`Erreur TAF ${icaoCode}:`, error.message);
    return false;
  }
}

// Fonction principale
async function updateMeteoData() {
  console.log("Début de la mise à jour des données météo");
  const startTime = new Date();
  
  try {
    // Récupérer tous les codes OACI
    const { data: aerodromes, error } = await supabase
      .from('aerodromes')
      .select('code_oaci');
    
    if (error) throw error;
    
    const validAerodromes = aerodromes.filter(a => a.code_oaci && a.code_oaci.length === 4);
    console.log(`Trouvé ${validAerodromes.length} aérodromes valides sur ${aerodromes.length} total`);
    
    // Statistiques globales
    let metarSuccess = 0;
    let tafSuccess = 0;
    let processed = 0;
    let errors = 0;
    
    // Traitement par lots en parallèle
    for (let i = 0; i < validAerodromes.length; i += CONCURRENT_BATCH_SIZE) {
      const batchStartTime = new Date();
      
      // Prendre un lot d'aérodromes
      const batch = validAerodromes.slice(i, i + CONCURRENT_BATCH_SIZE);
      console.log(`Traitement du lot ${Math.floor(i/CONCURRENT_BATCH_SIZE) + 1}/${Math.ceil(validAerodromes.length/CONCURRENT_BATCH_SIZE)} (${batch.length} aérodromes)`);
      
      // Traiter tous les aérodromes du lot en parallèle
      const results = await Promise.all(
        batch.map(async (aerodrome) => {
          const icaoCode = aerodrome.code_oaci;
          let result = { icaoCode, metarSuccess: false, tafSuccess: false, error: null };
          
          try {
            // Récupérer et mettre à jour METAR et TAF en parallèle
            const [metarResult, tafResult] = await Promise.all([
              updateMetar(icaoCode),
              updateTaf(icaoCode)
            ]);
            
            result.metarSuccess = metarResult;
            result.tafSuccess = tafResult;
          } catch (error) {
            result.error = error.message;
            console.error(`Erreur pour ${icaoCode}:`, error.message);
          }
          
          return result;
        })
      );
      
      // Calculer les statistiques pour ce lot
      const batchResults = results.reduce((stats, result) => {
        if (result.metarSuccess) stats.metarSuccess++;
        if (result.tafSuccess) stats.tafSuccess++;
        if (result.error) stats.errors++;
        stats.processed++;
        return stats;
      }, { metarSuccess: 0, tafSuccess: 0, processed: 0, errors: 0 });
      
      // Mettre à jour les statistiques globales
      metarSuccess += batchResults.metarSuccess;
      tafSuccess += batchResults.tafSuccess;
      processed += batchResults.processed;
      errors += batchResults.errors;
      
      // Afficher la progression et le temps
      const batchEndTime = new Date();
      const batchDuration = (batchEndTime - batchStartTime) / 1000;
      console.log(`Lot terminé en ${batchDuration.toFixed(2)}s - Progression: ${processed}/${validAerodromes.length} aérodromes traités`);
      console.log(`METAR: ${batchResults.metarSuccess}/${batch.length} succès, TAF: ${batchResults.tafSuccess}/${batch.length} succès, Erreurs: ${batchResults.errors}`);
    }
    
    // Statistiques finales
    const endTime = new Date();
    const totalDuration = (endTime - startTime) / 1000;
    console.log("========= RÉCAPITULATIF =========");
    console.log(`Terminé en ${totalDuration.toFixed(2)} secondes`);
    console.log(`Total: ${processed} aérodromes traités (${errors} avec erreurs)`);
    console.log(`METAR: ${metarSuccess}/${validAerodromes.length} mis à jour (${((metarSuccess/validAerodromes.length)*100).toFixed(1)}%)`);
    console.log(`TAF: ${tafSuccess}/${validAerodromes.length} mis à jour (${((tafSuccess/validAerodromes.length)*100).toFixed(1)}%)`);
    console.log("=================================");
    
  } catch (error) {
    console.error("Erreur globale:", error);
    process.exit(1);
  }
}

// Exécuter le script
updateMeteoData();
