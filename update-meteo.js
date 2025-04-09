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

// Paramètres optimisés
const BATCH_SIZE = 10;                  // Nombre d'aérodromes par lot
const PROBLEMATIC_BATCH_SIZE = 3;       // Taille réduite pour les aérodromes problématiques
const PAUSE_BETWEEN_AERODROMES = 300;   // Pause entre aérodromes (ms)
const PAUSE_BETWEEN_OPERATIONS = 200;   // Pause entre opérations (ms)
const PAUSE_BETWEEN_LOTS = 2000;        // Pause entre lots (ms)

// Liste des aérodromes problématiques
const PROBLEMATIC_AERODROMES = [
  'EDDB', 'EDDC', 'EDDF', 'EDDG', 'EDDH', 'EDDK', 'EDDL', 'EDDM', 
  'EDDP', 'EDDS', 'EDDV', 'EDDW', 'EGAA', 'EGBB', 'EGCC', 'EGFF',
  'EGGD', 'EGGP', 'EGLL', 'EGNM', 'EGNX', 'EGNT', 'EGPD', 'EGPH'
];

/**
 * Fonction pour nettoyer le texte TAF (supprime les duplications de "TAF")
 */
function cleanTafText(rawText) {
  // Supprimer les espaces supplémentaires
  let cleanedText = rawText.replace(/\s+/g, ' ').trim();
  
  // Corriger le problème "TAF TAF" en double
  if (cleanedText.startsWith('TAF TAF')) {
    cleanedText = cleanedText.replace('TAF TAF', 'TAF');
  }
  
  return cleanedText;
}

/**
 * Fonction robuste pour extraire les données de vent
 */
function extractWindData(text) {
  // Valeurs par défaut
  const result = {
    ventDirection: null,
    ventVariable: false,
    ventVitesse: null,
    ventRafales: null,
    ventUnites: 'KT'
  };
  
  if (!text) return result;
  
  // Diviser en mots
  const words = text.split(' ');
  
  // Chercher le motif de vent dans chaque mot
  for (const word of words) {
    // Cas 1: Direction variable (VRB)
    const vrbMatch = word.match(/^VRB(\d{2,3})(G(\d{2,3}))?(KT|MPS)$/);
    if (vrbMatch) {
      result.ventVariable = true;
      result.ventDirection = null;
      result.ventVitesse = parseInt(vrbMatch[1], 10);
      if (vrbMatch[3]) result.ventRafales = parseInt(vrbMatch[3], 10);
      result.ventUnites = vrbMatch[4];
      return result;
    }
    
    // Cas 2: Direction inconnue
    const unknownMatch = word.match(/^\/\/\/(\d{2,3})(G(\d{2,3}))?(KT|MPS)$/);
    if (unknownMatch) {
      result.ventDirection = null;
      result.ventVitesse = parseInt(unknownMatch[1], 10);
      if (unknownMatch[3]) result.ventRafales = parseInt(unknownMatch[3], 10);
      result.ventUnites = unknownMatch[4];
      return result;
    }
    
    // Cas 3: Calme (pas de vent)
    if (word === '00000KT' || word === '00000MPS') {
      result.ventDirection = 0;
      result.ventVitesse = 0;
      result.ventUnites = word.endsWith('KT') ? 'KT' : 'MPS';
      return result;
    }
    
    // Cas 4: Format standard
    const standardMatch = word.match(/^(\d{3})(\d{2,3})(G(\d{2,3}))?(KT|MPS)$/);
    if (standardMatch) {
      result.ventDirection = parseInt(standardMatch[1], 10);
      result.ventVitesse = parseInt(standardMatch[2], 10);
      if (standardMatch[4]) result.ventRafales = parseInt(standardMatch[4], 10);
      result.ventUnites = standardMatch[5];
      return result;
    }
  }
  
  return result;
}

/**
 * Fonction robuste pour extraire la visibilité
 */
function extractVisibility(text) {
  if (!text) return null;
  
  const words = text.split(' ');
  
  for (const word of words) {
    // Cas CAVOK
    if (word === 'CAVOK' || word === 'SKC' || word === 'CLR' || word === 'NSC') {
      return 9999;
    }
    
    // Cas 4 chiffres
    if (/^\d{4}$/.test(word) && parseInt(word, 10) <= 9999) {
      return parseInt(word, 10);
    }
    
    // Cas 9999
    if (word === '9999') {
      return 9999;
    }
  }
  
  return null;
}

/**
 * Fonction pour analyser un METAR
 */
async function updateMetar(icaoCode, isProblematic = false) {
  try {
    // Récupérer le METAR
    const url = `https://tgftp.nws.noaa.gov/data/observations/metar/stations/${icaoCode}.TXT`;
    const response = await fetch(url);
    
    if (!response.ok) return false;
    
    const text = await response.text();
    const lines = text.trim().split('\n');
    
    if (lines.length < 2) return false;
    
    const dateStr = lines[0].trim();
    const rawMetar = lines[1].trim();
    const observationDate = new Date(dateStr);
    
    // 1. Version rapide et simplifiée pour les aérodromes problématiques
    if (isProblematic) {
      const { error } = await supabase
        .from('metars')
        .upsert({
          code_oaci: icaoCode,
          raw_metar: rawMetar,
          date_observation: observationDate.toISOString(),
          updated_at: new Date().toISOString()
        }, { onConflict: 'code_oaci' });
      
      if (error) throw error;
      return true;
    }
    
    // 2. Version complète pour les aérodromes standards
    // D'abord, insérer les données brutes
    const { error: rawError } = await supabase
      .from('metars')
      .upsert({
        code_oaci: icaoCode,
        raw_metar: rawMetar,
        date_observation: observationDate.toISOString(),
        updated_at: new Date().toISOString()
      }, { onConflict: 'code_oaci' });
    
    if (rawError) throw rawError;
    
    // Pause entre opérations
    await new Promise(r => setTimeout(r, PAUSE_BETWEEN_OPERATIONS));
    
    // Extraire les données de vent
    const windData = extractWindData(rawMetar);
    const visibility = extractVisibility(rawMetar);
    
    // Mettre à jour avec les données extraites (pas de JSON complexe)
    const { error: updateError } = await supabase
      .from('metars')
      .update({
        vent_direction: windData.ventDirection,
        vent_variable: windData.ventVariable,
        vent_vitesse: windData.ventVitesse,
        vent_rafales: windData.ventRafales,
        vent_unites: windData.ventUnites,
        visibilite: visibility
      })
      .eq('code_oaci', icaoCode);
    
    if (updateError) throw updateError;
    
    return true;
  } catch (error) {
    console.error(`Erreur METAR ${icaoCode}:`, error.message);
    return false;
  }
}

/**
 * Fonction pour découper un TAF en segments
 */
async function processTafSegments(tafData, icaoCode) {
  try {
    const { raw_taf, validite_debut, validite_fin, id } = tafData;
    
    // Supprimer les segments existants
    await supabase
      .from('taf_segments')
      .delete()
      .eq('taf_id', id);
    
    // Nettoyer le texte TAF
    const cleanedTafText = cleanTafText(raw_taf || '');
    if (!cleanedTafText) return 0;
    
    // Découper en segments
    const tokens = cleanedTafText.split(' ');
    let segments = [];
    let currentSegment = {
      type: 'INIT',
      startIndex: 0,
      tokens: []
    };
    
    // Trouver les changements
    for (let i = 0; i < tokens.length; i++) {
      const token = tokens[i];
      
      // Détecter les marqueurs de changement
      if (token === 'BECMG' || token === 'TEMPO' || 
          token.startsWith('FM') || token.startsWith('PROB')) {
        
        // Terminer le segment courant
        if (currentSegment.tokens.length > 0) {
          segments.push({
            ...currentSegment,
            text: currentSegment.tokens.join(' ')
          });
        }
        
        // Définir le nouveau type de segment
        let segmentType = token;
        let probability = null;
        
        if (token.startsWith('PROB')) {
          segmentType = 'PROB';
          probability = parseInt(token.substring(4), 10);
        }
        
        // Commencer un nouveau segment
        currentSegment = {
          type: segmentType,
          startIndex: i,
          tokens: [token],
          probability: probability
        };
      } else {
        // Ajouter au segment courant
        currentSegment.tokens.push(token);
      }
    }
    
    // Ajouter le dernier segment
    if (currentSegment.tokens.length > 0) {
      segments.push({
        ...currentSegment,
        text: currentSegment.tokens.join(' ')
      });
    }
    
    // Insérer les segments un par un
    for (const segment of segments) {
      // Extraire les données de vent et visibilité
      const windData = extractWindData(segment.text);
      const visibility = extractVisibility(segment.text);
      
      // Insérer le segment
      await supabase.from('taf_segments').insert({
        taf_id: id,
        code_oaci: icaoCode,
        segment_type: segment.type,
        probability: segment.probability,
        valide_debut: validite_debut,
        valide_fin: validite_fin,
        raw_segment: segment.text,
        vent_direction: windData.ventDirection,
        vent_variable: windData.ventVariable,
        vent_vitesse: windData.ventVitesse,
        vent_rafales: windData.ventRafales,
        vent_unites: windData.ventUnites,
        visibilite: visibility
      });
      
      // Pause entre insertions
      await new Promise(r => setTimeout(r, 100));
    }
    
    return segments.length;
  } catch (error) {
    console.error(`Erreur lors du traitement des segments TAF pour ${icaoCode}:`, error.message);
    return 0;
  }
}

/**
 * Fonction pour mettre à jour un TAF
 */
async function updateTaf(icaoCode, isProblematic = false) {
  try {
    const url = `https://tgftp.nws.noaa.gov/data/forecasts/taf/stations/${icaoCode}.TXT`;
    const response = await fetch(url);
    
    if (!response.ok) return false;
    
    const text = await response.text();
    const lines = text.trim().split('\n');
    
    if (lines.length < 2) return false;
    
    const dateStr = lines[0].trim();
    let rawTaf = lines.slice(1).join(' ').trim();
    const emissionDate = new Date(dateStr);
    
    // Nettoyer le texte TAF (corriger TAF TAF)
    rawTaf = cleanTafText(rawTaf);
    
    // Extraire la période de validité
    let validiteDebut = null;
    let validiteFin = null;
    
    const validityMatch = rawTaf.match(/\b(\d{2})(\d{2})\/(\d{2})(\d{2})\b/);
    if (validityMatch) {
      const now = new Date();
      const year = now.getUTCFullYear();
      const month = now.getUTCMonth();
      
      const day1 = parseInt(validityMatch[1], 10);
      const hour1 = parseInt(validityMatch[2], 10);
      const day2 = parseInt(validityMatch[3], 10);
      const hour2 = parseInt(validityMatch[4], 10);
      
      validiteDebut = new Date(Date.UTC(year, month, day1, hour1, 0, 0));
      validiteFin = new Date(Date.UTC(year, month, day2, hour2, 0, 0));
      
      if (validiteFin < validiteDebut) {
        validiteFin.setUTCMonth(validiteFin.getUTCMonth() + 1);
      }
    }
    
    // 1. Insérer les données brutes
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
    
    // Pause entre opérations
    await new Promise(r => setTimeout(r, PAUSE_BETWEEN_OPERATIONS));
    
    // 2. Traiter les segments pour les aérodromes non problématiques
    if (!isProblematic && data && data.length > 0) {
      const tafData = data[0];
      const segmentsCount = await processTafSegments(tafData, icaoCode);
      console.log(`TAF pour ${icaoCode} découpé en ${segmentsCount} segments`);
    }
    
    return true;
  } catch (error) {
    console.error(`Erreur TAF ${icaoCode}:`, error.message);
    return false;
  }
}

/**
 * Fonction principale pour mettre à jour les données météo
 */
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
    
    // Trier les aérodromes: d'abord les non problématiques, puis les problématiques
    const sortedAerodromes = [...validAerodromes].sort((a, b) => {
      const aIsProblematic = PROBLEMATIC_AERODROMES.includes(a.code_oaci);
      const bIsProblematic = PROBLEMATIC_AERODROMES.includes(b.code_oaci);
      return aIsProblematic - bIsProblematic; // false(0) avant true(1)
    });
    
    // Statistiques
    let metarSuccess = 0;
    let tafSuccess = 0;
    let processed = 0;
    
    // Traiter les aérodromes par lots
    for (let i = 0; i < sortedAerodromes.length;) {
      // Déterminer si le lot contient des aérodromes problématiques
      const hasProblematicAerodromes = sortedAerodromes.slice(i).some(
        a => PROBLEMATIC_AERODROMES.includes(a.code_oaci)
      );
      
      // Adapter la taille du lot
      const currentBatchSize = hasProblematicAerodromes ? 
        PROBLEMATIC_BATCH_SIZE : BATCH_SIZE;
      
      const batch = sortedAerodromes.slice(i, i + currentBatchSize);
      const batchNumber = Math.floor(i / currentBatchSize) + 1;
      const totalBatches = Math.ceil(sortedAerodromes.length / currentBatchSize);
      
      console.log(`Traitement du lot ${batchNumber}/${totalBatches} (${batch.length} aérodromes)`);
      const batchStartTime = new Date();
      
      // Traitement séquentiel du lot
      for (const aerodrome of batch) {
        const icaoCode = aerodrome.code_oaci;
        const isProblematic = PROBLEMATIC_AERODROMES.includes(icaoCode);
        
        if (isProblematic) {
          console.log(`Traitement simplifié pour aérodrome problématique: ${icaoCode}`);
        }
        
        try {
          // METAR
          const metarResult = await updateMetar(icaoCode, isProblematic);
          if (metarResult) metarSuccess++;
          
          // Pause entre METAR et TAF
          await new Promise(r => setTimeout(r, PAUSE_BETWEEN_OPERATIONS));
          
          // TAF
          const tafResult = await updateTaf(icaoCode, isProblematic);
          if (tafResult) tafSuccess++;
          
          processed++;
        } catch (error) {
          console.error(`Erreur pour ${icaoCode}:`, error.message);
        }
        
        // Pause entre aérodromes
        await new Promise(r => setTimeout(r, PAUSE_BETWEEN_AERODROMES));
      }
      
      // Avancer au prochain lot
      i += currentBatchSize;
      
      // Statistiques du lot
      const batchEndTime = new Date();
      const batchDuration = (batchEndTime - batchStartTime) / 1000;
      
      console.log(`Lot terminé en ${batchDuration.toFixed(2)}s - Progression: ${processed}/${sortedAerodromes.length} aérodromes traités`);
      console.log(`METAR: ${metarSuccess}, TAF: ${tafSuccess}`);
      
      // Pause entre les lots
      if (i < sortedAerodromes.length) {
        console.log(`Pause de ${PAUSE_BETWEEN_LOTS/1000}s avant le prochain lot...`);
        await new Promise(r => setTimeout(r, PAUSE_BETWEEN_LOTS));
      }
    }
    
    // Statistiques finales
    const endTime = new Date();
    const totalDuration = (endTime - startTime) / 1000;
    
    console.log("========= RÉCAPITULATIF =========");
    console.log(`Terminé en ${totalDuration.toFixed(2)} secondes`);
    console.log(`Total: ${processed}/${sortedAerodromes.length} aérodromes traités`);
    console.log(`METAR: ${metarSuccess}/${sortedAerodromes.length} mis à jour (${((metarSuccess/sortedAerodromes.length)*100).toFixed(1)}%)`);
    console.log(`TAF: ${tafSuccess}/${sortedAerodromes.length} mis à jour (${((tafSuccess/sortedAerodromes.length)*100).toFixed(1)}%)`);
    console.log("=================================");
    
  } catch (error) {
    console.error("Erreur globale:", error);
    process.exit(1);
  }
}

// Exécuter le script
updateMeteoData();
