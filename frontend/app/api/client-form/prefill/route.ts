import { NextRequest, NextResponse } from 'next/server'

export async function POST(request: NextRequest) {
  try {
    const { address } = await request.json()

    if (!address) {
      return NextResponse.json(
        { error: 'Address is required' },
        { status: 400 }
      )
    }

    // API credentials
    const SMARTY_AUTH_ID = process.env.SMARTY_AUTH_ID
    const SMARTY_AUTH_TOKEN = process.env.SMARTY_AUTH_TOKEN
    const GOOGLE_MAPS_API_KEY = process.env.GOOGLE_MAPS_API_KEY

    // Fetch data from both APIs in parallel
    const [smartyData, googleData] = await Promise.all([
      fetchSmartyData(address, SMARTY_AUTH_ID, SMARTY_AUTH_TOKEN),
      fetchGoogleMapsData(address, GOOGLE_MAPS_API_KEY)
    ])

    let fieldsCount = 0
    const data: any = {}

    // Extract Smarty property data if available (enrichment API structure)
    if (smartyData) {
      const attributes = smartyData.attributes || {}
      const matchedAddress = smartyData.matched_address || {}
      
      console.log('📦 Extracting Smarty property data...')
      console.log('   - Deed Owner:', attributes.deed_owner_full_name)
      console.log('   - Company Flag:', attributes.company_flag)
      console.log('   - Ownership Type:', attributes.ownership_type)
      console.log('   - Corporation Name Field:', attributes.corporation_name)
      
      // === PROPERTY DETAILS ===
      if (attributes.building_sqft) {
        data.buildingSquareFootage = attributes.building_sqft.toString()
        fieldsCount++
      }
      
      if (attributes.year_built) {
        data.yearBuilt = attributes.year_built.toString()
        fieldsCount++
      }
      
      if (attributes.construction_type) {
        data.constructionType = mapConstructionType(attributes.construction_type)
        fieldsCount++
      }
      
      if (attributes.stories) {
        data.stories = attributes.stories
        fieldsCount++
      }
      
      if (attributes.acres) {
        data.acres = attributes.acres.toString()
        fieldsCount++
      }
      
      // === OWNER INFORMATION ===
      if (attributes.deed_owner_full_name || attributes.owner_full_name) {
        data.ownerName = attributes.deed_owner_full_name || attributes.owner_full_name
        fieldsCount++
      }
      
      // Corporation name - use corporation_name if available, otherwise use deed_owner_full_name if it's a company
      if (attributes.corporation_name) {
        data.corporationName = attributes.corporation_name
        fieldsCount++
        console.log('   ✅ Corporation Name extracted (from corporation_name):', attributes.corporation_name)
      } else if ((attributes.deed_owner_full_name || attributes.owner_full_name) && 
                 (attributes.company_flag === 'owner_is_company' || attributes.ownership_type === 'company')) {
        // If owner is a company, use deed owner name as corporation name
        data.corporationName = attributes.deed_owner_full_name || attributes.owner_full_name
        fieldsCount++
        console.log('   ✅ Corporation Name extracted (from deed_owner_full_name):', data.corporationName)
      }
      
      if (attributes.ownership_type) {
        data.ownershipType = attributes.ownership_type
        fieldsCount++
        console.log('   ✅ Ownership Type extracted:', attributes.ownership_type)
      }
      
      // === FINANCIAL/MORTGAGE INFO ===
      if (attributes.lender_name) {
        data.lenderName = attributes.lender_name
        fieldsCount++
      }
      
      if (attributes.mortgage_amount) {
        data.mortgageAmount = attributes.mortgage_amount.toString()
        fieldsCount++
      }
      
      if (attributes.assessed_value) {
        data.assessedValue = attributes.assessed_value.toString()
        fieldsCount++
      }
      
      // === LAND USE ===
      if (attributes.land_use_standard) {
        data.landUse = attributes.land_use_standard
        fieldsCount++
      }
      
      if (attributes.land_use_group) {
        data.landUseGroup = attributes.land_use_group
        fieldsCount++
      }
      
      // === PROTECTION CLASS (if available) ===
      if (attributes.legal_description) {
        data.protectionClass = attributes.legal_description
        fieldsCount++
      }
      
      // === LOCATION DATA ===
      if (matchedAddress.latitude && matchedAddress.longitude) {
        data.latitude = matchedAddress.latitude
        data.longitude = matchedAddress.longitude
        fieldsCount++
      }
    }

    // Extract Google Maps business data if available
    if (googleData && googleData.business) {
      const business = googleData.business
      
      console.log('📦 Extracting Google Maps business data...')
      console.log('   - Business Name:', business.name)
      console.log('   - Phone:', business.formatted_phone_number)
      console.log('   - Types:', business.types)
      
      // === BUSINESS NAME (DBA) ===
      if (business.name) {
        data.dba = business.name
        data.businessName = business.name
        fieldsCount++
        console.log('   ✅ DBA extracted:', business.name)
      }
      
      // === CONTACT INFO ===
      if (business.formatted_phone_number) {
        data.phoneNumber = business.formatted_phone_number
        data.contactNumber = business.formatted_phone_number
        fieldsCount++
        console.log('   ✅ Phone extracted:', business.formatted_phone_number)
      }
      
      // === HOURS OF OPERATION ===
      if (business.opening_hours) {
        // Check if open 24 hours
        if (business.opening_hours.periods && business.opening_hours.periods.length === 1) {
          const period = business.opening_hours.periods[0]
          if (!period.close) {
            data.hoursOfOperation = '24'
            data.hoursText = '24 Hours (Open 24/7)'
            data.is24Hours = true
            fieldsCount++
            console.log('   ✅ Hours extracted: 24 (Open 24/7)')
          }
        } else if (business.opening_hours.periods && business.opening_hours.periods.length > 0) {
          // Calculate average daily hours from periods
          const dailyHours = calculateDailyHours(business.opening_hours.periods)
          data.hoursOfOperation = dailyHours.toString()
          data.hoursText = formatOpeningHours(business.opening_hours)
          fieldsCount++
          console.log('   ✅ Hours extracted:', dailyHours, 'hours per day')
        } else if (business.opening_hours.weekday_text) {
          // Fallback: parse from text
          const hours = formatOpeningHours(business.opening_hours)
          data.hoursOfOperation = parseHoursFromText(business.opening_hours.weekday_text[0]) || hours
          data.hoursText = hours
          fieldsCount++
          console.log('   ✅ Hours extracted (from text):', data.hoursOfOperation)
        }
      }
      
      // === OPERATION DESCRIPTION ===
      let operationDesc = ''
      if (business.types && business.types.length > 0) {
        const types = business.types
          .filter((t: string) => !['point_of_interest', 'establishment'].includes(t))
          .map((t: string) => t.replace(/_/g, ' '))
          .slice(0, 3)
          .join(', ')
        
        // Determine if it's a gas station/convenience store
        const isGasStation = business.types.some((t: string) => 
          t === 'gas_station' || t === 'convenience_store'
        )
        
        if (isGasStation) {
          operationDesc = 'C-Store with'
          if (data.is24Hours) {
            operationDesc += ' 24 hours operation'
          } else {
            operationDesc += ` operations`
          }
        } else {
          operationDesc = `Business Type: ${types}`
        }
      }
      
      if (business.editorial_summary?.overview) {
        operationDesc += (operationDesc ? '. ' : '') + business.editorial_summary.overview
      }
      
      if (operationDesc) {
        data.operationDescription = operationDesc
        fieldsCount++
      }
      
      // === WEBSITE ===
      if (business.website) {
        data.website = business.website
        fieldsCount++
      }
      
      // === RATING & REVIEWS ===
      if (business.rating) {
        data.googleRating = business.rating
        data.totalReviews = business.user_ratings_total || 0
        fieldsCount++
      }
      
      // === BUSINESS STATUS ===
      if (business.business_status) {
        data.businessStatus = business.business_status
        data.currentlyOpen = business.business_status === 'OPERATIONAL'
        fieldsCount++
      }
    }

    // Return success even if no data was found
    return NextResponse.json({
      success: fieldsCount > 0,
      message: fieldsCount > 0 
        ? `Found property data and pre-filled ${fieldsCount} fields`
        : 'No property data found. You can continue filling the form manually.',
      fieldsCount,
      data: fieldsCount > 0 ? data : null,
      address: address
    })

  } catch (error) {
    console.error('API Error:', error)
    // Return success: false but with 200 status so the app continues
    return NextResponse.json({
      success: false,
      message: 'Unable to fetch property data. You can continue filling the form manually.',
      fieldsCount: 0,
      data: null,
      error: error instanceof Error ? error.message : 'Unknown error'
    })
  }
}

// Helper function to fetch Smarty property data
async function fetchSmartyData(address: string, authId: string | undefined, authToken: string | undefined): Promise<any> {
  try {
    if (!authId || !authToken) {
      console.log('Smarty credentials not configured')
      return null
    }

    // Use the enrichment API to get property principal data
    const principalUrl = `https://us-enrichment.api.smarty.com/lookup/search/property/principal`
    const params = new URLSearchParams({
      freeform: address,
      'auth-id': authId,
      'auth-token': authToken
    })

    console.log(`📡 Making Smarty API request for: ${address}`)
    
    const smartyResponse = await fetch(`${principalUrl}?${params.toString()}`, { 
      signal: AbortSignal.timeout(5000), // 5 second timeout
      headers: {
        'Accept': 'application/json'
      }
    })
    
    if (smartyResponse.ok) {
      const data = await smartyResponse.json()
      console.log('✅ Smarty data fetched successfully:', data)
      return data && data.length > 0 ? data[0] : null
    } else {
      console.warn('Smarty API returned error:', smartyResponse.status, smartyResponse.statusText)
      return null
    }
  } catch (error) {
    console.warn('Smarty API unavailable:', error)
    return null
  }
}

// Helper function to fetch Google Maps data
async function fetchGoogleMapsData(address: string, apiKey: string | undefined): Promise<any> {
  try {
    if (!apiKey) {
      console.log('Google Maps API key not configured')
      return null
    }

    // Step 1: Geocode the address
    const geocodeUrl = `https://maps.googleapis.com/maps/api/geocode/json?address=${encodeURIComponent(address)}&key=${apiKey}`
    const geocodeResponse = await fetch(geocodeUrl, {
      signal: AbortSignal.timeout(5000)
    })
    const geocodeData = await geocodeResponse.json()

    if (geocodeData.status !== 'OK' || !geocodeData.results.length) {
      console.log('Google Geocoding failed:', geocodeData.status)
      return null
    }

    const location = geocodeData.results[0].geometry.location
    const placeId = geocodeData.results[0].place_id
    
    console.log(`📍 Geocoded address - Lat: ${location.lat}, Lng: ${location.lng}, Place ID: ${placeId}`)

    // Step 2: Search for nearby businesses (convenience stores, gas stations) at this location
    const nearbySearchUrl = `https://maps.googleapis.com/maps/api/place/nearbysearch/json?location=${location.lat},${location.lng}&radius=50&key=${apiKey}`
    const nearbyResponse = await fetch(nearbySearchUrl, {
      signal: AbortSignal.timeout(5000)
    })
    const nearbyData = await nearbyResponse.json()

    console.log('🔍 Nearby search results:', nearbyData.results?.length || 0, 'places found')
    if (nearbyData.results && nearbyData.results.length > 0) {
      console.log('🏪 Nearby businesses:', nearbyData.results.map((p: any) => ({ name: p.name, types: p.types })))
    }

    let businessPlaceId = placeId
    let businessName = ''

    // Look for convenience store, gas station, or store at this location
    if (nearbyData.status === 'OK' && nearbyData.results && nearbyData.results.length > 0) {
      // Prioritize convenience stores and gas stations
      const relevantBusiness = nearbyData.results.find((place: any) => 
        place.types?.includes('convenience_store') || 
        place.types?.includes('gas_station') ||
        place.types?.includes('store')
      ) || nearbyData.results[0] // Fallback to first result

      if (relevantBusiness) {
        businessPlaceId = relevantBusiness.place_id
        businessName = relevantBusiness.name
        console.log('🎯 Selected business:', businessName, '- Types:', relevantBusiness.types)
      }
    }

    // Step 3: Get place details for business information
    const detailsUrl = `https://maps.googleapis.com/maps/api/place/details/json?place_id=${businessPlaceId}&fields=name,formatted_phone_number,opening_hours,types,business_status,rating,website,editorial_summary,user_ratings_total&key=${apiKey}`
    const detailsResponse = await fetch(detailsUrl, {
      signal: AbortSignal.timeout(5000)
    })
    const detailsData = await detailsResponse.json()
    
    console.log('🗺️ Google Maps Place Details Response:', JSON.stringify(detailsData, null, 2))

    if (detailsData.status === 'OK' && detailsData.result) {
      console.log('✅ Google Maps data fetched successfully')
      console.log('📊 Business Name (DBA):', detailsData.result.name)
      console.log('📞 Phone Number:', detailsData.result.formatted_phone_number)
      console.log('🏢 Business Types:', detailsData.result.types)
      return {
        business: detailsData.result,
        location: location
      }
    }

    console.warn('⚠️ Google Maps Place Details returned status:', detailsData.status)

    return null
  } catch (error) {
    console.warn('Google Maps API unavailable:', error)
    return null
  }
}

// Helper function to map Smarty construction types to our form options
function mapConstructionType(smartyType: string): string {
  const type = smartyType.toLowerCase()
  
  if (type.includes('masonry') || type.includes('brick')) {
    return 'Masonry'
  } else if (type.includes('frame') || type.includes('wood')) {
    return 'Frame'
  } else if (type.includes('fire') || type.includes('resistive')) {
    return 'Fire Resistive'
  } else if (type.includes('steel') || type.includes('concrete')) {
    return 'Non-Combustible'
  }
  
  return 'Frame' // Default
}

// Helper function to format hours of operation
function formatOpeningHours(openingHours: any): string {
  if (!openingHours || !openingHours.weekday_text) {
    return ''
  }
  return openingHours.weekday_text.join('; ')
}

// Helper function to calculate daily operating hours from Google Maps periods
function calculateDailyHours(periods: any[]): number {
  if (!periods || periods.length === 0) return 0
  
  // If there's only one period with no close time, it's 24 hours
  if (periods.length === 1 && !periods[0].close) {
    return 24
  }
  
  // Calculate hours for each day and get average
  const dailyHoursMap: { [key: number]: number } = {}
  
  periods.forEach((period: any) => {
    if (!period.open || !period.close) return
    
    const day = period.open.day
    const openTime = parseInt(period.open.time)
    const closeTime = parseInt(period.close.time)
    
    // Calculate hours (handle overnight periods)
    let hours = 0
    if (closeTime > openTime) {
      hours = (closeTime - openTime) / 100
    } else {
      // Overnight (e.g., open at 8am, close at 2am next day)
      hours = ((2400 - openTime) + closeTime) / 100
    }
    
    dailyHoursMap[day] = (dailyHoursMap[day] || 0) + hours
  })
  
  // Get average hours per day
  const totalHours = Object.values(dailyHoursMap).reduce((sum, h) => sum + h, 0)
  const avgHours = totalHours / Object.keys(dailyHoursMap).length
  
  // Round to nearest whole number
  return Math.round(avgHours)
}

// Helper function to parse hours from text like "8:00 AM – 12:00 AM"
function parseHoursFromText(hoursText: string): string | null {
  if (!hoursText) return null
  
  // Check for 24 hours
  if (hoursText.toLowerCase().includes('24 hours') || hoursText.toLowerCase().includes('open 24')) {
    return '24'
  }
  
  // Extract time range like "8:00 AM – 12:00 AM"
  const timeMatch = hoursText.match(/(\d{1,2}):(\d{2})\s*(AM|PM)\s*[–-]\s*(\d{1,2}):(\d{2})\s*(AM|PM)/i)
  if (timeMatch) {
    const openHour = parseInt(timeMatch[1])
    const openMin = parseInt(timeMatch[2])
    const openPeriod = timeMatch[3].toUpperCase()
    const closeHour = parseInt(timeMatch[4])
    const closeMin = parseInt(timeMatch[5])
    const closePeriod = timeMatch[6].toUpperCase()
    
    // Convert to 24-hour format
    let openTime24 = openHour
    if (openPeriod === 'PM' && openHour !== 12) openTime24 += 12
    if (openPeriod === 'AM' && openHour === 12) openTime24 = 0
    
    let closeTime24 = closeHour
    if (closePeriod === 'PM' && closeHour !== 12) closeTime24 += 12
    if (closePeriod === 'AM' && closeHour === 12) closeTime24 = 0
    
    // Calculate hours
    let hours = 0
    if (closeTime24 > openTime24) {
      hours = closeTime24 - openTime24 + (closeMin - openMin) / 60
    } else {
      // Overnight
      hours = (24 - openTime24) + closeTime24 + (closeMin - openMin) / 60
    }
    
    return Math.round(hours).toString()
  }
  
  return null
}
