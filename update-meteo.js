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
const CONCURRENT_BATCH_SIZE = 10; // Réduit à 10 pour éviter les surcharges
const INITIAL_BATCH_SIZE = 5;     // Premier lot encore plus petit
const RETRY_ATTEMPTS = 5;         // Augmenté à 5 pour plus de réessais
const RETRY_DELAY = 1000;         // Délai initial de 1 seconde
const MAX_RETRY_DELAY = 10000;    // Délai maximum de 10 secondes

// Fonction avec retry pour les appels HTTP
async function fetchWithRetry(url, retries = RETRY_ATTEMPTS, delay = RETRY_DELAY) {
  try {
    const response = await fetch(url);
    return response;
  } catch (error) {
    if (retries <= 1) throw error;
    console.log(`Réessai de récupération de ${url} dans ${delay/1000}s...`);
    await new Promise(resolve => setTimeout(resolve, delay));
    return fetchWithRetry(url, retries - 1, Math.min(delay * 2, MAX_RETRY_DELAY));
  }
}

// Fonction pour effectuer des opérations Supabase avec retry
async function supabaseWithRetry(operation, retries = RETRY_ATTEMPTS, delay = RETRY_DELAY) {
  try {
    return await operation();
  } catch (error) {
    if (error.message && error.message.includes('timeout') && retries > 1) {
      console.log(`Timeout Supabase, réessai dans ${delay/1000}s...`);
      await new Promise(resolve => setTimeout(resolve, delay));
      return supabaseWithRetry(operation, retries - 1, Math.min(delay * 2, MAX_RETRY_DELAY));
    }
    throw error;
  }
}

// Fonction pour analyser en profondeur un METAR
function parseMetarDetails(rawMetar) {
  // (Le code reste le même)
}

// Fonction pour mettre à jour un METAR avec retry
async function updateMetar(icaoCode, isFirstBatch = false) {
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
    
    // Analyser le METAR
    const parsedMetar = parseMetarDetails(rawMetar);
    
    // Version simplifiée pour les premiers lots (pour éviter les timeouts)
    let metarData;
    
    if (isFirstBatch) {
      // Version simplifiée avec moins de données JSON complexes
      metarData = {
        code_oaci: icaoCode,
        raw_metar: rawMetar,
        date_observation: observationDate.toISOString(),
        vent_direction: parsedMetar.ventDirection,
        vent_variable: parsedMetar.ventVariable, 
        vent_vitesse: parsedMetar.ventVitesse,
        vent_rafales: parsedMetar.ventRafales,
        vent_unites: parsedMetar.ventUnites,
        visibilite: parsedMetar.visibilite,
        temperature: parsedMetar.temperature,
        point_rosee: parsedMetar.pointRosee,
        qnh: parsedMetar.qnh,
        updated_at: new Date().toISOString()
      };
    } else {
      // Version complète avec toutes les données
      metarData = {
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
      };
    }
    
    // Insérer dans Supabase avec retry
    const { error } = await supabaseWithRetry(async () => {
      return await supabase
        .from('metars')
        .upsert(metarData, { onConflict: 'code_oaci' });
    });
    
    if (error) throw error;
    
    return true;
  } catch (error) {
    console.error(`Erreur METAR ${icaoCode}:`, error.message);
    return false;
  }
}

// Fonction améliorée pour analyser les segments TAF
async function parseTafSegments(tafData, icaoCode, isFirstBatch = false) {
  try {
    const { raw_taf, validite_debut, validite_fin, id, date_emission } = tafData;
    
    // Supprimer les segments existants pour ce TAF
    await supabaseWithRetry(async () => {
      return await supabase
        .from('taf_segments')
        .delete()
        .eq('taf_id', id);
    });
    
    // Si c'est le premier lot, on ne fait qu'une analyse simple
    if (isFirstBatch) {
      // Insertion simple d'un seul segment basique
      await supabaseWithRetry(async () => {
        return await supabase
          .from('taf_segments')
          .insert({
            taf_id: id,
            code_oaci: icaoCode,
            segment_type: 'INIT',
            raw_segment: raw_taf,
            valide_debut: validite_debut,
            valide_fin: validite_fin
          });
      });
      
      return 1; // Un seul segment
    }
    
    // Le reste du code reste le même pour l'analyse complète
    // ...
  } catch (error) {
    console.error(`Erreur lors de l'analyse des segments TAF pour ${icaoCode}:`, error.message);
    return 0;
  }
}

// Fonction pour mettre à jour le TAF avec retry
async function updateTaf(icaoCode, isFirstBatch = false) {
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
    
    // Insérer/mettre à jour le TAF principal avec retry
    const { data, error } = await supabaseWithRetry(async () => {
      return await supabase
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
    });
    
    if (error) throw error;
    
    // Analyser et insérer les segments du TAF
    if (data && data.length > 0) {
      const tafData = data[0];
      const segmentsCount = await parseTafSegments(tafData, icaoCode, isFirstBatch);
      if (!isFirstBatch) {
        console.log(`TAF pour ${icaoCode} découpé en ${segmentsCount} segments`);
      }
    }
    
    return true;
  } catch (error) {
    console.error(`Erreur TAF ${icaoCode}:`, error.message);
    return false;
  }
}

// Fonction principale avec traitement adaptatif
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
    
    // Traitement spécial du premier lot pour "préchauffer" la connexion
    const firstBatchSize = Math.min(INITIAL_BATCH_SIZE, validAerodromes.length);
    if (firstBatchSize > 0) {
      console.log(`Traitement du lot d'initialisation (${firstBatchSize} aérodromes)`);
      const firstBatch = validAerodromes.slice(0, firstBatchSize);
      
      // Traiter les aérodromes un par un avec un petit délai entre chaque
      for (const aerodrome of firstBatch) {
        const icaoCode = aerodrome.code_oaci;
        try {
          await updateMetar(icaoCode, true);
          await updateTaf(icaoCode, true);
          // Pause entre chaque aérodrome
          await new Promise(resolve => setTimeout(resolve, 500));
        } catch (e) {
          console.error(`Erreur d'initialisation pour ${icaoCode}:`, e.message);
        }
      }
      
      console.log(`Lot d'initialisation terminé, passage au traitement principal`);
      processed += firstBatchSize;
    }
    
    // Traitement des lots restants en parallèle
    const remainingAerodromes = validAerodromes.slice(firstBatchSize);
    for (let i = 0; i < remainingAerodromes.length; i += CONCURRENT_BATCH_SIZE) {
      const batchStartTime = new Date();
      
      // Prendre un lot d'aérodromes
      const batch = remainingAerodromes.slice(i, i + CONCURRENT_BATCH_SIZE);
      const batchNumber = Math.floor(i/CONCURRENT_BATCH_SIZE) + 1;
      const totalBatches = Math.ceil(remainingAerodromes.length/CONCURRENT_BATCH_SIZE);
      console.log(`Traitement du lot ${batchNumber}/${totalBatches} (${batch.length} aérodromes)`);
      
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
    console.log(`METAR: ${metarSuccess}/${validAerodromes.length - firstBatchSize} mis à jour (${((metarSuccess/(validAerodromes.length - firstBatchSize))*100).toFixed(1)}%)`);
    console.log(`TAF: ${tafSuccess}/${validAerodromes.length - firstBatchSize} mis à jour (${((tafSuccess/(validAerodromes.length - firstBatchSize))*100).toFixed(1)}%)`);
    console.log("=================================");
    
  } catch (error) {
    console.error("Erreur globale:", error);
    process.exit(1);
  }
}

// Exécuter le script
updateMeteoData();
