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
  } catch (error) {
    // Log silencieux pour éviter trop de sorties en cas de multiples erreurs
    return false;
  }
}

// Fonction pour mettre à jour un TAF
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
    
    const { error } = await supabase
      .from('tafs')
      .upsert({
        code_oaci: icaoCode,
        raw_taf: rawTaf,
        date_emission: emissionDate.toISOString(),
        updated_at: new Date().toISOString()
      }, { onConflict: 'code_oaci' });
    
    if (error) throw error;
    
    return true;
  } catch (error) {
    // Log silencieux pour éviter trop de sorties en cas de multiples erreurs
    return false;
  }
}

// Exécuter le script
updateMeteoData();
