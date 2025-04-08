import { createClient } from '@supabase/supabase-js';
import fetch from 'node-fetch';

// Configuration Supabase
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

// Reste du code...

const { createClient } = require('@supabase/supabase-js')
const fetch = require('node-fetch')

// Configuration Supabase
const supabaseUrl = process.env.SUPABASE_URL
const supabaseKey = process.env.SUPABASE_KEY
const supabase = createClient(supabaseUrl, supabaseKey)

// Fonction principale
async function updateMeteoData() {
  console.log("Début de la mise à jour des données météo")
  
  try {
    // Récupérer tous les codes OACI
    const { data: aerodromes, error } = await supabase
      .from('aerodromes')
      .select('code_oaci')
    
    if (error) throw error
    
    console.log(`Trouvé ${aerodromes.length} aérodromes`)
    
    // Compteurs
    let processed = 0
    let metarSuccess = 0
    let tafSuccess = 0
    
    // Traiter chaque aérodrome
    for (const aerodrome of aerodromes) {
      const icaoCode = aerodrome.code_oaci
      
      if (!icaoCode || icaoCode.length !== 4) continue
      
      try {
        // Traiter METAR
        const metarUpdated = await updateMetar(icaoCode)
        if (metarUpdated) metarSuccess++
        
        // Traiter TAF
        const tafUpdated = await updateTaf(icaoCode)
        if (tafUpdated) tafSuccess++
        
        processed++
        
        // Afficher progression
        if (processed % 10 === 0) {
          console.log(`Progression: ${processed}/${aerodromes.length} aérodromes traités`)
        }
        
        // Petite pause pour ne pas surcharger
        await new Promise(resolve => setTimeout(resolve, 100))
      } catch (error) {
        console.error(`Erreur pour ${icaoCode}:`, error.message)
      }
    }
    
    console.log(`Terminé! ${processed} aérodromes traités: ${metarSuccess} METARs et ${tafSuccess} TAFs mis à jour`)
  } catch (error) {
    console.error("Erreur:", error)
    process.exit(1)
  }
}

// Traiter un METAR
async function updateMetar(icaoCode) {
  const url = `https://tgftp.nws.noaa.gov/data/observations/metar/stations/${icaoCode}.TXT`
  
  try {
    const response = await fetch(url)
    if (!response.ok) return false
    
    const text = await response.text()
    const lines = text.trim().split('\n')
    
    if (lines.length < 2) return false
    
    const dateStr = lines[0].trim()
    const rawMetar = lines[1].trim()
    const observationDate = new Date(dateStr)
    
    const { error } = await supabase
      .from('metars')
      .upsert({
        code_oaci: icaoCode,
        raw_metar: rawMetar,
        date_observation: observationDate.toISOString(),
        updated_at: new Date().toISOString()
      }, { onConflict: 'code_oaci' })
    
    if (error) throw error
    
    return true
  } catch (error) {
    console.error(`Erreur METAR ${icaoCode}:`, error.message)
    return false
  }
}

// Traiter un TAF
async function updateTaf(icaoCode) {
  const url = `https://tgftp.nws.noaa.gov/data/forecasts/taf/stations/${icaoCode}.TXT`
  
  try {
    const response = await fetch(url)
    if (!response.ok) return false
    
    const text = await response.text()
    const lines = text.trim().split('\n')
    
    if (lines.length < 2) return false
    
    const dateStr = lines[0].trim()
    const rawTaf = lines.slice(1).join(' ').trim()
    const emissionDate = new Date(dateStr)
    
    const { error } = await supabase
      .from('tafs')
      .upsert({
        code_oaci: icaoCode,
        raw_taf: rawTaf,
        date_emission: emissionDate.toISOString(),
        updated_at: new Date().toISOString()
      }, { onConflict: 'code_oaci' })
    
    if (error) throw error
    
    return true
  } catch (error) {
    console.error(`Erreur TAF ${icaoCode}:`, error.message)
    return false
  }
}

// Exécuter le script
updateMeteoData()
