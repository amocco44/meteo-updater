import { createClient } from '@supabase/supabase-js';
import fetch from 'node-fetch';

// Configuration Supabase
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!supabaseUrl || !supabaseKey) {
  console.error("Variables d'environnement manquantes");
  process.exit(1);
}

const supabase = createClient(supabaseUrl, supabaseKey);

// Paramètres
const BATCH_SIZE = 5; // Petits lots pour éviter les timeouts
const PAUSE_BETWEEN_AERODROMES = 300; // ms
const PAUSE_BETWEEN_LOTS = 2000; // ms

// Fonction robuste pour analyser les METAR
function parseMetarDetails(rawMetar) {
  try {
    // Valeurs par défaut pour éviter les erreurs undefined
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
    
    if (!rawMetar) return result;
    
    // Diviser le METAR en parties
    const parts = rawMetar.split(' ');
    if (parts.length < 3) return result; // METAR trop court
    
    // Ignorer le code OACI et la date/heure
    let i = 2;
    
    while (i < parts.length) {
      const part = parts[i];
      if (!part) { i++; continue; } // Ignorer les parties vides
      
      // Analyse du vent
      if (/^(\d{3}|VRB)(\d{2,3})(G\d{2,3})?(KT|MPS)$/.test(part) || 
          /^\/\/\/(\d{2,3})(G\d{2,3})?(KT|MPS)$/.test(part) || 
          /^00000(KT|MPS)$/.test(part)) {
        
        try {
          if (part.startsWith('VRB')) {
            result.ventVariable = true;
            const match = part.match(/VRB(\d{2,3})(G(\d{2,3}))?(KT|MPS)/);
            if (match) {
              result.ventVitesse = parseInt(match[1]);
              if (match[3]) result.ventRafales = parseInt(match[3]);
              result.ventUnites = match[4];
            }
          } else if (part.startsWith('///')) {
            const match = part.match(/\/\/\/(\d{2,3})(G(\d{2,3}))?(KT|MPS)/);
            if (match) {
              result.ventVitesse = parseInt(match[1]);
              if (match[3]) result.ventRafales = parseInt(match[3]);
              result.ventUnites = match[4];
            }
          } else if (part === '00000KT' || part === '00000MPS') {
            result.ventDirection = 0;
            result.ventVitesse = 0;
            result.ventUnites = part.endsWith('KT') ? 'KT' : 'MPS';
          } else {
            const match = part.match(/^(\d{3})(\d{2,3})(G(\d{2,3}))?(KT|MPS)$/);
            if (match) {
              result.ventDirection = parseInt(match[1]);
              result.ventVitesse = parseInt(match[2]);
              if (match[4]) result.ventRafales = parseInt(match[4]);
              result.ventUnites = match[5];
            }
          }
        } catch (e) {
          console.log(`Erreur analyse vent: ${e.message}`);
        }
      }
      // Analyse de la visibilité
      else if (/^\d{4}$/.test(part) || part === '9999') {
        try {
          result.visibilite = parseInt(part);
        } catch (e) { }
      }
      else if (part === 'CAVOK') {
        result.visibilite = 9999;
      }
      // Analyse des nuages
      else if (/^(FEW|SCT|BKN|OVC)(\d{3})(CB|TCU)?$/.test(part)) {
        try {
          const match = part.match(/^(FEW|SCT|BKN|OVC)(\d{3})(CB|TCU)?$/);
          if (match) {
            result.nuages.push({
              type: match[1],
              hauteur: parseInt(match[2]) * 100,
              orage: match[3] === 'CB' || match[3] === 'TCU'
            });
          }
        } catch (e) { }
      }
      // Température et point de rosée
      else if (/^(M)?(\d{1,2})\/(M)?(\d{1,2})$/.test(part)) {
        try {
          const match = part.match(/^(M)?(\d{1,2})\/(M)?(\d{1,2})$/);
          if (match) {
            result.temperature = (match[1] ? -1 : 1) * parseInt(match[2]);
            result.pointRosee = (match[3] ? -1 : 1) * parseInt(match[4]);
          }
        } catch (e) { }
      }
      // QNH
      else if (/^Q\d{4}$/.test(part)) {
        try {
          result.qnh = parseInt(part.substring(1));
        } catch (e) { }
      }
      
      i++;
    }
    
    return result;
  } catch (error) {
    console.error("Erreur analyse METAR:", error.message);
    // Retourner un objet vide mais valide
    return {
      ventDirection: null,
      ventVariable: false,
      ventVitesse: null,
      ventRafales: null,
      ventUnites: null,
      visibilite: null,
      phenomenes: [],
      nuages: [],
      temperature: null,
      pointRosee: null,
      qnh: null
    };
  }
}

// Fonction METAR complète mais robuste
async function updateMetar(icaoCode) {
  try {
    const url = `https://tgftp.nws.noaa.gov/data/observations/metar/stations/${icaoCode}.TXT`;
    const response = await fetch(url);
    
    if (!response.ok) return false;
    
    const text = await response.text();
    const lines = text.trim().split('\n');
    
    if (lines.length < 2) return false;
    
    const dateStr = lines[0].trim();
    const rawMetar = lines[1].trim();
    const observationDate = new Date(dateStr);
    
    // 1. D'abord juste sauvegarder les données brutes
    const { error: rawError } = await supabase
      .from('metars')
      .upsert({
        code_oaci: icaoCode,
        raw_metar: rawMetar,
        date_observation: observationDate.toISOString(),
        updated_at: new Date().toISOString()
      }, { onConflict: 'code_oaci' });
    
    if (rawError) throw rawError;
    
    // 2. Ensuite faire l'analyse détaillée
    const parsedMetar = parseMetarDetails(rawMetar);
    
    // 3. Mettre à jour avec l'analyse
    const { error: updateError } = await supabase
      .from('metars')
      .update({
        vent_direction: parsedMetar.ventDirection,
        vent_variable: parsedMetar.ventVariable, 
        vent_vitesse: parsedMetar.ventVitesse,
        vent_rafales: parsedMetar.ventRafales,
        vent_unites: parsedMetar.ventUnites,
        visibilite: parsedMetar.visibilite,
        phenomenes: parsedMetar.phenomenes.length > 0 ? parsedMetar.phenomenes : null,
        nuages: parsedMetar.nuages.length > 0 ? parsedMetar.nuages : null,
        temperature: parsedMetar.temperature,
        point_rosee: parsedMetar.pointRosee,
        qnh: parsedMetar.qnh
      })
      .eq('code_oaci', icaoCode);
    
    if (updateError) throw updateError;
    
    return true;
  } catch (error) {
    console.error(`Erreur METAR ${icaoCode}:`, error.message);
    return false;
  }
}

// Fonction pour analyser un TAF de manière robuste
async function analyzeTafAndCreateSegments(tafData, icaoCode) {
  try {
    const { raw_taf, validite_debut, validite_fin, id } = tafData;
    
    // Supprimer les segments existants
    try {
      await supabase
        .from('taf_segments')
        .delete()
        .eq('taf_id', id);
    } catch (deleteError) {
      console.error(`Erreur suppression segments TAF ${icaoCode}:`, deleteError.message);
    }
    
    if (!raw_taf) return 0;
    
    // Expressions régulières pour les changements
    const changeRegex = /\b(BECMG|TEMPO|FM\d{6}|PROB\d{2})\b/g;
    
    // Diviser le TAF en tokens
    const tokens = raw_taf.split(' ');
    
    // Trouver les indices des tokens de changement
    const changes = [];
    let currentType = 'INIT';
    let currentStart = 0;
    
    for (let i = 0; i < tokens.length; i++) {
      const token = tokens[i];
      if (token.match(/^(BECMG|TEMPO)$/) || token.match(/^FM\d{6}$/) || token.match(/^PROB\d{2}$/)) {
        // Enregistrer le segment précédent
        changes.push({
          type: currentType,
          startIndex: currentStart,
          endIndex: i - 1
        });
        
        // Nouveau segment
        currentType = token.match(/^PROB\d{2}$/) ? 'PROB' : token;
        currentStart = i;
      }
    }
    
    // Ajouter le dernier segment
    changes.push({
      type: currentType,
      startIndex: currentStart,
      endIndex: tokens.length - 1
    });
    
    // Traiter chaque segment
    for (let i = 0; i < changes.length; i++) {
      const segment = changes[i];
      const segmentText = tokens.slice(segment.startIndex, segment.endIndex + 1).join(' ');
      
      // Extraire les données météo basiques du segment
      let ventDirection = null;
      let ventVariable = false;
      let ventVitesse = null;
      let ventRafales = null;
      let ventUnites = 'KT';
      let visibilite = null;
      let probability = null;
      
      // Calculer la probabilité si c'est un segment PROB
      if (segment.type === 'PROB' && tokens[segment.startIndex].match(/^PROB(\d{2})$/)) {
        try {
          probability = parseInt(tokens[segment.startIndex].substring(4));
        } catch (e) {}
      }
      
      // Extraire les infos météo
      for (let j = segment.startIndex; j <= segment.endIndex; j++) {
        const token = tokens[j];
        
        // Vent
        if (/^(\d{3}|VRB)(\d{2,3})(G\d{2,3})?(KT|MPS)$/.test(token)) {
          try {
            if (token.startsWith('VRB')) {
              ventVariable = true;
              const match = token.match(/VRB(\d{2,3})(G(\d{2,3}))?(KT|MPS)/);
              if (match) {
                ventVitesse = parseInt(match[1]);
                if (match[3]) ventRafales = parseInt(match[3]);
                ventUnites = match[4];
              }
            } else {
              const match = token.match(/^(\d{3})(\d{2,3})(G(\d{2,3}))?(KT|MPS)$/);
              if (match) {
                ventDirection = parseInt(match[1]);
                ventVitesse = parseInt(match[2]);
                if (match[4]) ventRafales = parseInt(match[4]);
                ventUnites = match[5];
              }
            }
          } catch (e) {}
        }
        
        // Visibilité
        if (/^\d{4}$/.test(token)) {
          try {
            visibilite = parseInt(token);
          } catch (e) {}
        }
        
        if (token === 'CAVOK' || token === '9999') {
          visibilite = 9999;
        }
      }
      
      // Insérer le segment
      try {
        await supabase.from('taf_segments').insert({
          taf_id: id,
          code_oaci: icaoCode,
          segment_type: segment.type,
          probability: probability,
          valide_debut: validite_debut,
          valide_fin: validite_fin,
          raw_segment: segmentText,
          vent_direction: ventDirection,
          vent_variable: ventVariable,
          vent_vitesse: ventVitesse,
          vent_rafales: ventRafales,
          vent_unites: ventUnites,
          visibilite: visibilite
        });
      } catch (insertError) {
        console.error(`Erreur insertion segment TAF ${icaoCode}:`, insertError.message);
      }
    }
    
    return changes.length;
  } catch (error) {
    console.error(`Erreur analyse TAF ${icaoCode}:`, error.message);
    return 0;
  }
}

// Fonction TAF complète en approche séquentielle
async function updateTaf(icaoCode) {
  try {
    const url = `https://tgftp.nws.noaa.gov/data/forecasts/taf/stations/${icaoCode}.TXT`;
    const response = await fetch(url);
    
    if (!response.ok) return false;
    
    const text = await response.text();
    const lines = text.trim().split('\n');
    
    if (lines.length < 2) return false;
    
    const dateStr = lines[0].trim();
    const rawTaf = lines.slice(1).join(' ').trim();
    const emissionDate = new Date(dateStr);
    
    // Extraire la période de validité
    let validiteDebut = null;
    let validiteFin = null;
    
    const validityMatch = rawTaf.match(/\b(\d{2})(\d{2})\/(\d{2})(\d{2})\b/);
    if (validityMatch) {
      const now = new Date();
      const year = now.getUTCFullYear();
      const month = now.getUTCMonth();
      
      const day1 = parseInt(validityMatch[1]);
      const hour1 = parseInt(validityMatch[2]);
      const day2 = parseInt(validityMatch[3]);
      const hour2 = parseInt(validityMatch[4]);
      
      validiteDebut = new Date(Date.UTC(year, month, day1, hour1, 0, 0));
      validiteFin = new Date(Date.UTC(year, month, day2, hour2, 0, 0));
      
      if (validiteFin < validiteDebut) {
        validiteFin.setUTCMonth(validiteFin.getUTCMonth() + 1);
      }
    }
    
    // 1. D'abord sauvegarder le TAF brut
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
        returning: 'representation'
      });
    
    if (error) throw error;
    
    // 2. Ensuite analyser les segments si possible
    if (data && data.length > 0) {
      // Pause brève avant d'analyser les segments
      await new Promise(resolve => setTimeout(resolve, 100));
      
      const tafData = data[0];
      const segmentsCount = await analyzeTafAndCreateSegments(tafData, icaoCode);
    }
    
    return true;
  } catch (error) {
    console.error(`Erreur TAF ${icaoCode}:`, error.message);
    return false;
  }
}

// Fonction principale avec approche séquentielle
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
    
    // Statistiques
    let metarSuccess = 0;
    let tafSuccess = 0;
    let processed = 0;
    
    // Traiter les aérodromes par petits lots séquentiels
    for (let i = 0; i < validAerodromes.length; i += BATCH_SIZE) {
      const batchStartTime = new Date();
      
      const batch = validAerodromes.slice(i, i + BATCH_SIZE);
      const batchNumber = Math.floor(i / BATCH_SIZE) + 1;
      const totalBatches = Math.ceil(validAerodromes.length / BATCH_SIZE);
      
      console.log(`Traitement du lot ${batchNumber}/${totalBatches} (${batch.length} aérodromes)`);
      
      // Traitement séquentiel à l'intérieur du lot
      for (const aerodrome of batch) {
        const icaoCode = aerodrome.code_oaci;
        
        try {
          // METAR
          const metarResult = await updateMetar(icaoCode);
          if (metarResult) metarSuccess++;
          
          // Pause entre METAR et TAF
          await new Promise(resolve => setTimeout(resolve, 100));
          
          // TAF
          const tafResult = await updateTaf(icaoCode);
          if (tafResult) tafSuccess++;
          
          processed++;
        } catch (error) {
          console.error(`Erreur pour ${icaoCode}:`, error.message);
        }
        
        // Pause entre chaque aérodrome
        await new Promise(resolve => setTimeout(resolve, PAUSE_BETWEEN_AERODROMES));
      }
      
      // Statistiques du lot
      const batchEndTime = new Date();
      const batchDuration = (batchEndTime - batchStartTime) / 1000;
      
      console.log(`Lot terminé en ${batchDuration.toFixed(2)}s - Progression: ${processed}/${validAerodromes.length} aérodromes traités`);
      console.log(`METAR: ${metarSuccess}, TAF: ${tafSuccess}`);
      
      // Pause entre les lots
      if (i + BATCH_SIZE < validAerodromes.length) {
        console.log(`Pause avant le prochain lot...`);
        await new Promise(resolve => setTimeout(resolve, PAUSE_BETWEEN_LOTS));
      }
    }
    
    // Statistiques finales
    const endTime = new Date();
    const totalDuration = (endTime - startTime) / 1000;
    
    console.log("========= RÉCAPITULATIF =========");
    console.log(`Terminé en ${totalDuration.toFixed(2)} secondes`);
    console.log(`Total: ${processed}/${validAerodromes.length} aérodromes traités`);
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
