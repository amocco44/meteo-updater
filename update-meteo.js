// Ajoutez cette fonction avant updateMeteoData()
async function parseTafSegments(tafData, icaoCode) {
  try {
    // Récupérer le TAF complet
    const { raw_taf, validite_debut, validite_fin, id } = tafData;
    
    // Supprimer les segments existants pour ce TAF
    await supabase
      .from('taf_segments')
      .delete()
      .eq('taf_id', id);
    
    // Découper le TAF en parties en utilisant les marqueurs de changement
    const tafText = raw_taf.trim();
    const segments = [];
    
    // Expressions régulières pour identifier les différents types de segments
    const markerRegex = /\b(BECMG|TEMPO|FM\d{6}|PROB\d{2})\b/g;
    let match;
    let lastIndex = 0;
    let lastType = 'INIT';
    let lastPosition = 0;
    
    // Trouver tous les marqueurs de changement et leurs positions
    const markers = [];
    while ((match = markerRegex.exec(tafText)) !== null) {
      markers.push({
        type: match[1],
        position: match.index,
        text: match[0]
      });
    }
    
    // Si aucun marqueur n'est trouvé, tout le TAF est un segment initial
    if (markers.length === 0) {
      segments.push({
        type: 'INIT',
        text: tafText,
        valid_from: validite_debut,
        valid_to: validite_fin
      });
    } else {
      // Ajouter le segment initial (avant le premier marqueur)
      segments.push({
        type: 'INIT',
        text: tafText.substring(0, markers[0].position).trim(),
        valid_from: validite_debut,
        valid_to: validite_fin
      });
      
      // Ajouter les segments intermédiaires
      for (let i = 0; i < markers.length; i++) {
        const nextPos = (i < markers.length - 1) ? markers[i + 1].position : tafText.length;
        
        segments.push({
          type: markers[i].type.startsWith('PROB') ? 'PROB' : markers[i].type,
          probability: markers[i].type.startsWith('PROB') ? parseInt(markers[i].type.substring(4)) : null,
          text: tafText.substring(markers[i].position, nextPos).trim(),
          valid_from: validite_debut,
          valid_to: validite_fin
        });
      }
    }
    
    // Extraire les informations météo de base pour chaque segment
    for (const segment of segments) {
      // Analyse basique du segment pour extraire quelques informations
      const parts = segment.text.split(' ');
      
      let ventDirection = null;
      let ventVitesse = null;
      let ventRafales = null;
      let visibilite = null;
      
      // Parcourir les parties pour extraire les informations
      for (const part of parts) {
        // Analyse du vent (format: dddssGgg)
        const windPattern = /^(\d{3}|VRB)(\d{2})(G(\d{2}))?KT$/;
        const windMatch = part.match(windPattern);
        
        if (windMatch) {
          ventDirection = windMatch[1] === 'VRB' ? 0 : parseInt(windMatch[1]);
          ventVitesse = parseInt(windMatch[2]);
          if (windMatch[4]) ventRafales = parseInt(windMatch[4]);
          continue;
        }
        
        // Analyse de la visibilité
        if (/^\d{4}$/.test(part)) {
          visibilite = parseInt(part);
          continue;
        }
        
        if (part === 'CAVOK') {
          visibilite = 9999;
          continue;
        }
      }
      
      // Insérer le segment dans la table taf_segments
      await supabase.from('taf_segments').insert({
        taf_id: id,
        code_oaci: icaoCode,
        segment_type: segment.type,
        probability: segment.probability,
        valide_debut: segment.valid_from,
        valide_fin: segment.valid_to,
        raw_segment: segment.text,
        vent_direction: ventDirection,
        vent_vitesse: ventVitesse,
        vent_rafales: ventRafales,
        visibilite: visibilite
      });
    }
    
    return segments.length;
  } catch (error) {
    console.error(`Erreur lors de l'analyse des segments TAF pour ${icaoCode}:`, error.message);
    return 0;
  }
}

// Modifiez la fonction updateTaf pour inclure le découpage
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
