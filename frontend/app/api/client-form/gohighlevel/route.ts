import { NextRequest, NextResponse } from 'next/server'

export async function POST(request: NextRequest) {
  try {
    const formData = await request.json()

    const GHL_API_KEY = process.env.GHL_API_KEY
    const GHL_LOCATION_ID = process.env.GHL_LOCATION_ID

    console.log('🔍 Environment Check:', {
      hasApiKey: !!GHL_API_KEY,
      hasLocationId: !!GHL_LOCATION_ID,
      apiKeyType: GHL_API_KEY?.substring(0, 20),
      apiKeyLength: GHL_API_KEY?.length,
      locationId: GHL_LOCATION_ID
    })

    if (!GHL_API_KEY) {
      console.error('❌ GoHighLevel API key not configured')
      return NextResponse.json(
        { success: false, error: 'GoHighLevel not configured - GHL_API_KEY missing' },
        { status: 400 }
      )
    }

    console.log('📤 Sending data to GoHighLevel CRM...')
    console.log('Form data received:', {
      contactName: formData.contactName,
      email: formData.contactEmail,
      phone: formData.contactNumber,
      address: formData.address
    })

    // Create contact with basic matching fields
    const contactData: any = {
      firstName: formData.contactName?.split(' ')[0] || '',
      lastName: formData.contactName?.split(' ').slice(1).join(' ') || '',
      name: formData.contactName || '',
      email: formData.contactEmail || '',
      phone: formData.contactNumber || '',
      address1: formData.address || '',
      companyName: formData.corporationName || formData.dba || '',
      source: 'Insurance Application Form',
      tags: ['insurance-lead', 'c-store']
    }

    // REQUIRED: locationId must be in the request
    if (!GHL_LOCATION_ID) {
      console.error('❌ Location ID is required but not configured')
      return NextResponse.json(
        { success: false, error: 'Location ID not configured' },
        { status: 400 }
      )
    }
    contactData.locationId = GHL_LOCATION_ID

    console.log('📡 Making API request to GoHighLevel...')
    console.log('API Key type:', GHL_API_KEY?.substring(0, 20) + '...')
    console.log('API Key length:', GHL_API_KEY?.length)
    console.log('Location ID:', GHL_LOCATION_ID)
    
    // Ensure Authorization header format is correct (no double "Bearer Bearer")
    let authHeader = GHL_API_KEY
    if (authHeader && !authHeader.startsWith('Bearer ')) {
      authHeader = `Bearer ${authHeader}`
    }
    
    console.log('Request headers:', {
      'Authorization': authHeader.substring(0, 30) + '...',
      'Content-Type': 'application/json',
      'Accept': 'application/json',
      'Version': '2021-07-28'
    })
    
    const ghlResponse = await fetch('https://services.leadconnectorhq.com/contacts/', {
      method: 'POST',
      headers: {
        'Authorization': authHeader,
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Version': '2021-07-28'
      },
      body: JSON.stringify(contactData)
    })

    const ghlData = await ghlResponse.json()
    
    console.log('📡 GoHighLevel Response:', {
      status: ghlResponse.status,
      statusText: ghlResponse.statusText,
      data: ghlData
    })
    
    // Check if token is expired
    if (ghlResponse.status === 401 && ghlData.message === 'Invalid JWT') {
      console.error('❌ Invalid JWT Error - Possible causes:')
      console.error('1. Token is expired')
      console.error('2. Token was revoked')
      console.error('3. Wrong API key type (need Private App Key, not OAuth token)')
      console.error('4. Location ID mismatch')
      console.error('💡 SOLUTION: Generate a new API key from Settings → API Keys in GoHighLevel')
    }

    if (ghlResponse.ok) {
      console.log('✅ Contact created in GoHighLevel:', ghlData.contact?.id)
      
      if (ghlData.contact?.id) {
        await createOpportunityWithJSON(ghlData.contact.id, formData, GHL_API_KEY, GHL_LOCATION_ID)
      }

      return NextResponse.json({
        success: true,
        message: 'Lead saved to GoHighLevel CRM',
        contactId: ghlData.contact?.id
      })
    } else {
      console.error('❌ GoHighLevel API error:', ghlData)
      console.error('Request that failed:', {
        url: 'https://services.leadconnectorhq.com/contacts/',
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${GHL_API_KEY?.substring(0, 10)}...`,
          'Content-Type': 'application/json'
        },
        body: contactData
      })
      return NextResponse.json(
        { success: false, error: ghlData.message || `Failed to create contact: ${ghlResponse.statusText}` },
        { status: ghlResponse.status }
      )
    }

  } catch (error) {
    console.error('Error sending to GoHighLevel:', error)
    return NextResponse.json(
      { success: false, error: 'Internal server error' },
      { status: 500 }
    )
  }
}

// Create opportunity with mapped fields
async function createOpportunityWithJSON(contactId: string, formData: any, apiKey: string, locationId?: string) {
  try {
    // Check if pipeline IDs are configured
    const pipelineId = process.env.GHL_PIPELINE_ID
    const pipelineStageId = process.env.GHL_PIPELINE_STAGE_ID
    
    if (!pipelineId || !pipelineStageId) {
      console.warn('⚠️ Pipeline IDs not configured - skipping opportunity creation')
      console.warn('💡 Add GHL_PIPELINE_ID and GHL_PIPELINE_STAGE_ID to .env.local')
      return null
    }
    
    console.log('📊 Creating opportunity with:')
    console.log('   Pipeline:', pipelineId)
    console.log('   Stage:', pipelineStageId)
    console.log('   Contact:', contactId)
    
    const buildingCoverage = parseFloat(formData.building?.replace(/[^0-9.]/g, '') || '0')
    const bppCoverage = parseFloat(formData.bpp?.replace(/[^0-9.]/g, '') || '0')
    const biCoverage = parseFloat(formData.bi?.replace(/[^0-9.]/g, '') || '0')
    const totalCoverage = buildingCoverage + bppCoverage + biCoverage
    const estimatedValue = Math.round(totalCoverage * 0.01)
    
    console.log('💰 Coverage calculation:', { buildingCoverage, bppCoverage, biCoverage, totalCoverage, estimatedValue })

    const opportunityData: any = {
      contactId: contactId,
      name: `${formData.dba || formData.contactName} - C-Store Insurance`,
      pipelineId: pipelineId,
      pipelineStageId: pipelineStageId,
      status: 'open',
      monetaryValue: estimatedValue > 0 ? estimatedValue : 0,
    }

    if (locationId) {
      opportunityData.locationId = locationId
    }
    
    console.log('📤 Opportunity data:', opportunityData)

    // Ensure Authorization header has Bearer prefix for pit- tokens
    const oppAuthHeader = apiKey.startsWith('Bearer ') ? apiKey : `Bearer ${apiKey}`
    
    const oppResponse = await fetch('https://services.leadconnectorhq.com/opportunities/', {
      method: 'POST',
      headers: {
        'Authorization': oppAuthHeader,
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Version': '2021-07-28'
      },
      body: JSON.stringify(opportunityData)
    })

    if (oppResponse.ok) {
      const oppData = await oppResponse.json()
      const opportunityId = oppData.opportunity?.id
      console.log('✅ Opportunity created successfully!')
      console.log('   Opportunity ID:', opportunityId)
      
      if (opportunityId) {
        await addFormDataAsNote(opportunityId, formData, apiKey)
      }
      
      return opportunityId
    } else {
      const errorText = await oppResponse.text()
      console.error('❌ Failed to create opportunity!')
      console.error('   Status:', oppResponse.status, oppResponse.statusText)
      console.error('   Error response:', errorText)
      console.error('   Request data sent:', opportunityData)
      return null
    }
  } catch (error) {
    console.error('Error creating opportunity:', error)
    return null
  }
}

// Add complete form data as formatted note with JSON
async function addFormDataAsNote(opportunityId: string, formData: any, apiKey: string) {
  try {
    console.log('📝 Adding complete form data as JSON note...')
    
    const completeData = {
      submissionDate: new Date().toISOString(),
      formType: 'C-Store Insurance Application',
      
      personalInfo: {
        contactName: formData.contactName,
        contactEmail: formData.contactEmail,
        contactNumber: formData.contactNumber,
        corporationName: formData.corporationName,
      },
      
      companyInfo: {
        address: formData.address,
        dba: formData.dba,
        applicantType: formData.applicantType,
        yearsInBusiness: formData.yearsInBusiness,
        ownershipType: formData.ownershipType,
        operationDescription: formData.operationDescription,
      },
      
      propertyDetails: {
        hoursOfOperation: formData.hoursOfOperation,
        numberOfMPDs: formData.noOfMPDs,
        constructionType: formData.constructionType,
        totalSqFootage: formData.totalSqFootage,
        yearBuilt: formData.yearBuilt,
        yearsInCurrentLocation: formData.yearsAtLocation,
        leasedSpace: formData.anyLeasedOutSpace,
        protectionClass: formData.protectionClass,
        additionalInsured: formData.additionalInsured,
        burglarAlarm: formData.burglarAlarmCentral || formData.burglarAlarmLocal ? 'Yes' : 'No',
        fireAlarm: formData.fireAlarmCentral || formData.fireAlarmLocal ? 'Yes' : 'No',
        acres: formData.acres,
      },
      
      salesData: {
        inside: { monthly: formData.insideSalesMonthly, yearly: formData.insideSalesYearly },
        liquor: { monthly: formData.liquorSalesMonthly, yearly: formData.liquorSalesYearly },
        gasoline: { monthly: formData.gasolineSalesMonthly, yearly: formData.gasolineSalesYearly },
        propane: { monthly: formData.propaneFillingExchangeMonthly, yearly: formData.propaneFillingExchangeYearly },
        carwash: { monthly: formData.carwashMonthly, yearly: formData.carwashYearly },
        cooking: { monthly: formData.cookingMonthly, yearly: formData.cookingYearly },
      },
      
      coverage: {
        building: formData.building,
        bpp: formData.bpp,
        bi: formData.bi,
        canopy: formData.canopy,
        pumps: formData.pumps,
        signsLighting: formData.ms,
      },
      
      businessDetails: {
        fein: formData.fein,
        numberOfEmployees: formData.noOfEmployees,
        annualPayroll: formData.payroll,
        officers: formData.officersInclExcl,
        ownershipPercentage: formData.ownership,
        primaryLender: formData.primaryLender,
        mortgageAmount: formData.mortgageAmount,
      },
      
      metadata: {
        applicationStatus: formData.applicationStatus || 'Submitted',
        lastSavedStep: formData.lastSavedStep,
        lastSavedDate: formData.lastSavedDate,
      }
    }
    
    const noteBody = `## 📋 Complete Insurance Application Data

**Submission Date:** ${new Date().toISOString()}
**Status:** ${formData.applicationStatus || 'Submitted'}

### Quick Summary
- **Contact:** ${formData.contactName} - ${formData.contactEmail} - ${formData.contactNumber}
- **Business:** ${formData.dba || 'N/A'} (${formData.corporationName || 'N/A'})
- **Property:** ${formData.address}
- **Hours:** ${formData.hoursOfOperation || 'N/A'} hours/day
- **Coverage Total:** $${(parseFloat(formData.building?.replace(/[^0-9.]/g, '') || '0') + parseFloat(formData.bpp?.replace(/[^0-9.]/g, '') || '0') + parseFloat(formData.bi?.replace(/[^0-9.]/g, '') || '0')).toLocaleString()}

---

### 📎 Complete Application JSON

\`\`\`json
${JSON.stringify(completeData, null, 2)}
\`\`\`

> **Note:** Copy the JSON above and save as a file if needed for processing.`
    
    const noteData = {
      body: noteBody,
      opportunityId: opportunityId
    }
    
    // Ensure Authorization header has Bearer prefix
    const noteAuthHeader = apiKey.startsWith('Bearer ') ? apiKey : `Bearer ${apiKey}`
    
    const noteResponse = await fetch('https://services.leadconnectorhq.com/opportunities/' + opportunityId + '/notes', {
      method: 'POST',
      headers: {
        'Authorization': noteAuthHeader,
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Version': '2021-07-28'
      },
      body: JSON.stringify(noteData)
    })
    
    if (noteResponse.ok) {
      console.log('✅ JSON data added as note to opportunity')
    } else {
      const errorText = await noteResponse.text()
      console.warn('⚠️ Note creation failed:', errorText)
    }
  } catch (error) {
    console.error('Error adding note:', error)
  }
}
