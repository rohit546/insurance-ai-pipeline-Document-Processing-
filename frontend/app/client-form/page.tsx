'use client'

import { useState, useRef, useEffect } from 'react'
import { useForm } from 'react-hook-form'
import confetti from 'canvas-confetti'

type FormData = {
  // Address
  address: string
  
  // Company Information
  corporationName: string
  contactName: string
  contactNumber: string
  contactEmail: string
  proposedEffectiveDate: string
  priorCarrier: string
  targetPremium: string
  
  // Applicant Type
  applicantType: string
  
  // Security Systems
  burglarAlarmCentral: boolean
  burglarAlarmLocal: boolean
  fireAlarmCentral: boolean
  fireAlarmLocal: boolean
  
  // Operations
  operationDescription: string
  
  // Ownership
  ownershipType: string
  
  // Property Information
  yearBuilt: string
  totalSqFootage: string
  constructionType: string
  acres: string
  
  // Financial Information
  assessedValue: string
  marketValue: string
  primaryLender: string
  mortgageAmount: string
  
  // Location Information
  county: string
  municipality: string
  metroArea: string
  
  // Property Details
  bedrooms: string
  bathrooms: string
  stories: string
  garageSize: string
  lotSize: string
  
  // Construction Details
  roofType: string
  exteriorWalls: string
  foundation: string
  
  // Property Coverage
  building: string
  bpp: string
  bi: string
  canopy: string
  pumps: string
  ms: string
  
  // General Liability Sales (with monthly/yearly)
  insideSalesMonthly: string
  insideSalesYearly: string
  liquorSalesMonthly: string
  liquorSalesYearly: string
  gasolineSalesMonthly: string
  gasolineSalesYearly: string
  propaneFillingExchangeMonthly: string
  propaneFillingExchangeYearly: string
  carwashMonthly: string
  carwashYearly: string
  cookingMonthly: string
  cookingYearly: string
  
  // Business Details
  coverageLimits: string
  fein: string
  dba: string
  hoursOfOperation: string
  noOfMPDs: string
  yearsInBusiness: string
  yearsAtLocation: string
  yearOfLatestUpdate: string
  anyLeasedOutSpace: string
  protectionClass: string
  additionalInsured: string
  alarm: string
  noOfEmployees: string
  payroll: string
  officersInclExcl: string
  ownership: string
}

export default function Home() {
  const [currentStep, setCurrentStep] = useState(0)
  const [isSubmitted, setIsSubmitted] = useState(false)
  const [fetchedData, setFetchedData] = useState<any>(null)
  const [isLoading, setIsLoading] = useState(false)
  const [showSidePanel, setShowSidePanel] = useState(false)
  const [selectedEnrichmentFields, setSelectedEnrichmentFields] = useState<Record<string, boolean>>({})
  const addressInputRef = useRef<HTMLInputElement | null>(null)
  
  const totalSteps = 7
  const progress = currentStep > 0 ? ((currentStep - 1) / totalSteps) * 100 : 0
  
  const { register, handleSubmit, formState: { errors }, watch, setValue } = useForm<FormData>({
    mode: 'onChange',
    defaultValues: {
      address: '',
      applicantType: 'individual',
      burglarAlarmCentral: false,
      burglarAlarmLocal: false,
      fireAlarmCentral: false,
      fireAlarmLocal: false,
    }
  })
  
  // Register address field with proper ref handling
  const addressRegister = register('address', { 
    required: {
      value: true,
      message: 'Address is required'
    },
    validate: (value) => {
      if (!value) return 'Address is required'
      const trimmed = String(value).trim()
      if (trimmed.length === 0) {
        return 'Address cannot be empty'
      }
      return true
    }
  })

  // Initialize Google Places Autocomplete (optional - manual input also works)
  useEffect(() => {
    const timer = setTimeout(() => {
      if (window.google?.maps?.places && addressInputRef.current) {
        try {
          const autocomplete = new window.google.maps.places.Autocomplete(addressInputRef.current, {
            types: ['address'],
            componentRestrictions: { country: 'us' }
          })
          
          autocomplete.addListener('place_changed', () => {
            const place = autocomplete.getPlace()
            if (place.formatted_address) {
              setValue('address', place.formatted_address, { shouldValidate: true })
            }
          })
        } catch (error) {
          console.log('Autocomplete init skipped:', error)
        }
      }
    }, 1000)
    
    return () => clearTimeout(timer)
  }, [setValue])
  
  // Watch address value to debug
  const addressValue = watch('address')

  const fetchPropertyData = async (address?: string) => {
    if (!address || !address.trim()) return

    setIsLoading(true)
    setShowSidePanel(false)

    try {
      const response = await fetch('/api/client-form/prefill', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ address }),
      })

      const data = await response.json()
      setFetchedData(data)

      if (data.success && data.data) {
        // Don't auto-fill, just show the panel
        setShowSidePanel(true)
        
        // Initialize all available fields as selected by default
        const availableFields: Record<string, boolean> = {}
        
        // Property fields
        if (data.data.buildingSquareFootage) availableFields['totalSqFootage'] = true
        if (data.data.constructionType) availableFields['constructionType'] = true
        if (data.data.yearBuilt) availableFields['yearBuilt'] = true
        if (data.data.acres) availableFields['acres'] = true
        
        // Business fields
        if (data.data.dba) availableFields['dba'] = true
        if (data.data.hoursOfOperation) availableFields['hoursOfOperation'] = true
        if (data.data.operationDescription) availableFields['operationDescription'] = true
        if (data.data.phoneNumber) availableFields['phoneNumber'] = true
        
        // Owner/Corporation fields
        if (data.data.corporationName) availableFields['corporationName'] = true
        if (data.data.ownershipType) availableFields['ownershipType'] = true
        
        // Financial fields
        if (data.data.lenderName) availableFields['lenderName'] = true
        if (data.data.mortgageAmount) availableFields['mortgageAmount'] = true
        
        setSelectedEnrichmentFields(availableFields)
      }

      setCurrentStep(1)
    } catch (error) {
      console.error('Error fetching property data:', error)
      setCurrentStep(1)
    } finally {
      setIsLoading(false)
    }
  }

  const applySelectedEnrichments = () => {
    if (!fetchedData?.data) return
    
    const data = fetchedData.data
    
    // Property data from Smarty
    if (selectedEnrichmentFields['totalSqFootage'] && data.buildingSquareFootage) {
      setValue('totalSqFootage', data.buildingSquareFootage)
    }
    if (selectedEnrichmentFields['constructionType'] && data.constructionType) {
      setValue('constructionType', data.constructionType)
    }
    if (selectedEnrichmentFields['yearBuilt'] && data.yearBuilt) {
      setValue('yearBuilt', data.yearBuilt)
    }
    if (selectedEnrichmentFields['acres'] && data.acres) {
      setValue('acres', data.acres)
    }
    
    // Business data from Google Maps
    if (selectedEnrichmentFields['dba'] && data.dba) {
      setValue('dba', data.dba)
    }
    if (selectedEnrichmentFields['hoursOfOperation'] && data.hoursOfOperation) {
      setValue('hoursOfOperation', data.hoursOfOperation)
    }
    if (selectedEnrichmentFields['operationDescription'] && data.operationDescription) {
      setValue('operationDescription', data.operationDescription)
    }
    if (selectedEnrichmentFields['phoneNumber'] && data.phoneNumber) {
      setValue('contactNumber', data.phoneNumber)
    }
    
    // Owner/Corporation data
    if (selectedEnrichmentFields['corporationName'] && data.corporationName) {
      setValue('corporationName', data.corporationName)
    }
    if (selectedEnrichmentFields['ownershipType'] && data.ownershipType) {
      // Map Smarty ownership_type to form's applicantType
      const ownershipMapping: Record<string, string> = {
        'company': 'Corporation',
        'individual': 'Individual',
        'partnership': 'Partnership',
        'llc': 'LLC',
        'joint_venture': 'Joint Venture'
      }
      const mappedType = ownershipMapping[data.ownershipType.toLowerCase()] || 'Corporation'
      setValue('applicantType', mappedType)
    }
    
    // Financial data - Auto-generate Additional Insured from lender
    if (selectedEnrichmentFields['lenderName'] && data.lenderName) {
      setValue('primaryLender', data.lenderName)
      
      // Auto-generate Additional Insured field: "Lender Name - Mortgagee (Mortgage Amount)"
      if (data.mortgageAmount) {
        const formattedAmount = `$${parseInt(data.mortgageAmount).toLocaleString()}`
        setValue('additionalInsured', `${data.lenderName} - Mortgagee (${formattedAmount})`)
      } else {
        setValue('additionalInsured', `${data.lenderName} - Mortgagee`)
      }
    }
    if (selectedEnrichmentFields['mortgageAmount'] && data.mortgageAmount) {
      setValue('mortgageAmount', data.mortgageAmount)
    }

    // Close the panel after applying
    setShowSidePanel(false)
  }

  const toggleEnrichmentField = (fieldName: string) => {
    setSelectedEnrichmentFields(prev => ({
      ...prev,
      [fieldName]: !prev[fieldName]
    }))
  }

  // Get fields relevant to current step
  const getStepFields = (step: number) => {
    const data = fetchedData?.data
    if (!data) return []

    switch(step) {
      case 1: // Personal Information - Corporation Name + Phone Number
        return [
          { key: 'corporationName', label: 'Corporation Name', value: data.corporationName, section: '👤 Owner Information' },
          { key: 'phoneNumber', label: 'Phone Number', value: data.phoneNumber, section: '📞 Contact Information' },
        ].filter(f => f.value)
      
      case 2: // Company Information - DBA, Applicant Type, Operation Description
        return [
          { key: 'dba', label: 'Business Name (DBA)', value: data.dba, section: '💼 Business Information' },
          { key: 'ownershipType', label: 'Applicant Type', value: data.ownershipType, section: '👤 Owner Information' },
          { key: 'operationDescription', label: 'Operation Description', value: data.operationDescription, section: '💼 Business Information' },
        ].filter(f => f.value)
      
      case 3: // Property & Business Details - Hours, Construction, Sq Footage, Year Built, Additional Insured
        return [
          { key: 'hoursOfOperation', label: 'Hours of Operation', value: data.hoursOfOperation, section: '💼 Business Information' },
          { key: 'constructionType', label: 'Construction Type', value: data.constructionType, section: '🏢 Property Information' },
          { key: 'totalSqFootage', label: 'Total Sq. Footage', value: data.buildingSquareFootage, section: '🏢 Property Information' },
          { key: 'yearBuilt', label: 'Year Built', value: data.yearBuilt, section: '🏢 Property Information' },
          { key: 'lenderName', label: 'Additional Insured (Lender)', value: data.lenderName, section: '💰 Financial Information' },
          { key: 'mortgageAmount', label: 'Mortgage Amount', value: data.mortgageAmount, section: '💰 Financial Information' },
        ].filter(f => f.value)
      
      default:
        return [] // Steps 4-7 are manual fill (confidential data)
    }
  }

  const handleAddressSubmit = handleSubmit(async (data) => {
    const address = (data.address || '').trim()
    
    // Validate address is not empty (validation should already be done by react-hook-form)
    if (!address || address.length === 0) {
      return
    }
    
    // Fetch data from APIs and cache it (works with both Google autocomplete and manual input)
    await fetchPropertyData(address)
    
    // Move to Step 1 after data is fetched
    setCurrentStep(1)
  }, (errors) => {
    // Handle validation errors
    console.log('Validation errors:', errors)
  })

  const onSubmit = async (data: FormData) => {
    console.log('Form submitted:', data)
    
    // Send to GoHighLevel CRM
    try {
      const response = await fetch('/api/client-form/gohighlevel', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data),
      })
      
      const result = await response.json()
      
      if (result.success) {
        console.log('✅ Saved to GoHighLevel CRM:', result.contactId)
      } else {
        console.warn('⚠️ GoHighLevel save failed:', result.error)
      }
    } catch (error) {
      console.error('Error saving to CRM:', error)
    }
    
    setIsSubmitted(true)
    confetti({
      particleCount: 100,
      spread: 70,
      origin: { y: 0.6 }
    })
  }

  const handleSaveAndExit = async () => {
    const formData = watch() // Get all current form values
    
    console.log('💾 Saving progress to GoHighLevel CRM...')
    
    try {
      const response = await fetch('/api/client-form/gohighlevel', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          ...formData,
          applicationStatus: 'In Progress',
          lastSavedStep: currentStep,
          lastSavedDate: new Date().toISOString()
        }),
      })
      
      const result = await response.json()
      
      if (result.success) {
        alert('✅ Progress saved successfully! You can return anytime.')
        console.log('✅ Saved to GoHighLevel CRM:', result.contactId)
        
        // Optionally redirect or reset form
        // window.location.href = '/'
      } else {
        alert('⚠️ Could not save to CRM. Please try again.')
        console.warn('⚠️ GoHighLevel save failed:', result.error)
      }
    } catch (error) {
      console.error('Error saving to CRM:', error)
      alert('❌ Error saving progress. Please check your connection.')
    }
  }

  const nextStep = async () => {
    // Validate current step before proceeding
    let fieldsToValidate: (keyof FormData)[] = []
    
    switch(currentStep) {
      case 1: // Personal Information
        fieldsToValidate = ['contactName', 'contactNumber', 'contactEmail']
        break
      case 2: // Company Information - no required fields
        break
      case 3: // Property & Business Details - no required fields
        break
      case 4: // Sales Data - no required fields
        break
      case 5: // Property Coverage - no required fields
        break
      case 6: // Business Details - no required fields
        break
    }

    // Trigger validation for the fields
    if (fieldsToValidate.length > 0) {
      const result = await handleSubmit(
        () => {
          // Validation passed, move to next step
          if (currentStep < totalSteps) {
            setCurrentStep(currentStep + 1)
          }
        },
        (errors) => {
          // Validation failed, errors will be shown
          console.log('Validation errors:', errors)
        }
      )()
    } else {
      // No validation needed, proceed
      if (currentStep < totalSteps) {
        setCurrentStep(currentStep + 1)
      }
    }
  }

  const prevStep = () => {
    if (currentStep > 1) {
      setCurrentStep(currentStep - 1)
    }
  }

  // Get watched values for review
  const formData = watch()

  if (isSubmitted) {
    return (
      <div className="min-h-screen bg-white flex items-center justify-center p-4">
        <div className="max-w-2xl w-full text-center space-y-8">
          <div className="space-y-4">
            <div className="text-8xl">🎉</div>
            <h1 className="text-6xl font-black text-black">SUCCESS!</h1>
            <p className="text-2xl font-bold text-gray-700">
              Your insurance application has been submitted
            </p>
          </div>
          <button
            onClick={() => {
              setIsSubmitted(false)
              setCurrentStep(0)
            }}
            className="px-12 py-6 bg-black text-white text-xl font-black uppercase border-2 border-black rounded-2xl hover:bg-white hover:text-black transform hover:scale-105 transition-all duration-200 shadow-[8px_8px_0px_0px_rgba(0,0,0,1)]"
          >
            ← START NEW APPLICATION
          </button>
        </div>
      </div>
    )
  }

  if (currentStep === 0) {
    return (
      <div className="min-h-screen bg-gray-50 p-4 md:p-6">
        <div className="max-w-3xl mx-auto pt-12">
          {/* Branding Header */}
          <div className="text-center mb-12">
            <div className="mb-4">
              <h2 className="text-2xl font-bold text-black mb-1">McKinney & Co</h2>
              <p className="text-sm text-gray-600 uppercase tracking-wider">C-Store Insurance</p>
            </div>
            <div className="w-16 h-0.5 bg-black mx-auto mb-6"></div>
            <h1 className="text-3xl font-bold text-black mb-2">
              Insurance Application
            </h1>
            <p className="text-sm text-gray-600">
              Enter your property address to begin
            </p>
          </div>

          {/* Address Input Card */}
          <div className="bg-white rounded-xl p-8 mb-6">
            <form onSubmit={handleAddressSubmit} className="space-y-5">
              <div>
                <label className="block text-xs font-semibold text-gray-700 uppercase tracking-wide mb-2">
                  Property Address
                </label>
                <input
                  type="text"
                  {...(() => {
                    const { ref, ...rest } = addressRegister
                    return {
                      ...rest,
                      ref: (e: HTMLInputElement | null) => {
                        if (typeof ref === 'function') {
                          ref(e)
                        } else if (ref) {
                          (ref as React.MutableRefObject<HTMLInputElement | null>).current = e
                        }
                        (addressInputRef as React.MutableRefObject<HTMLInputElement | null>).current = e
                      }
                    }
                  })()}
                  placeholder="123 Main Street, City, State ZIP (or type manually)"
                  className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-all text-black placeholder-gray-400"
                />
                {errors.address && (
                  <p className="mt-2 text-xs text-red-600">{errors.address.message}</p>
                )}
              </div>

              <button
                type="submit"
                disabled={isLoading}
                className="w-full px-6 py-3.5 bg-black text-white text-sm font-semibold uppercase tracking-wide rounded-lg hover:bg-gray-800 transition-all disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {isLoading ? 'Searching...' : 'Start Application'}
              </button>
            </form>
          </div>

          {/* Feature Pills */}
          <div className="flex flex-wrap gap-3 justify-center">
            {['Fast Process', 'Secure', 'Auto-Fill'].map((feature) => (
              <div
                key={feature}
                className="px-4 py-2 bg-white rounded-full text-xs font-medium text-gray-700"
              >
                {feature}
              </div>
            ))}
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Data Enrichment Side Panel - Shows only fields relevant to current step */}
      {showSidePanel && fetchedData?.data && currentStep > 0 && currentStep <= 3 && (
        <div className="fixed left-0 top-0 h-full w-80 bg-white border-r border-gray-200 overflow-y-auto z-30 shadow-lg">
          <div className="sticky top-0 bg-white border-b border-gray-200 p-4 z-10">
            <div className="flex items-center justify-between mb-2">
              <div>
                <h3 className="text-lg font-bold text-black">Smart Suggestions</h3>
                {getStepFields(currentStep).length > 0 && (
                  <span className="text-xs text-gray-500">{getStepFields(currentStep).length} fields for this step</span>
                )}
              </div>
              <button
                onClick={() => setShowSidePanel(false)}
                className="w-8 h-8 flex items-center justify-center text-gray-400 hover:text-black hover:bg-gray-100 rounded-lg transition-colors"
              >
                ✕
              </button>
            </div>
            <p className="text-xs text-gray-500">Data fetched from property records & Google Maps</p>
          </div>

          <div className="p-4 space-y-4">
            {/* Render step-specific fields grouped by section */}
            {(() => {
              const stepFields = getStepFields(currentStep)
              if (stepFields.length === 0) {
                return (
                  <div className="text-center py-8 text-sm text-gray-500">
                    No additional data available for this step
                  </div>
                )
              }

              // Group fields by section
              const sections: Record<string, typeof stepFields> = {}
              stepFields.forEach(field => {
                if (!sections[field.section]) sections[field.section] = []
                sections[field.section].push(field)
              })

              return Object.entries(sections).map(([sectionName, sectionFields]) => (
                <div key={sectionName}>
                  <h4 className="text-xs font-bold text-gray-500 uppercase tracking-wider mb-2 px-1">{sectionName}</h4>
                  <div className="space-y-2">
                    {sectionFields.map(field => (
                      <label key={field.key} className="flex items-start gap-3 p-3 bg-gray-50 rounded-lg cursor-pointer hover:bg-gray-100 transition-colors">
                        <input 
                          type="checkbox" 
                          checked={selectedEnrichmentFields[field.key] || false} 
                          onChange={() => toggleEnrichmentField(field.key)} 
                          className="mt-1 w-4 h-4" 
                        />
                        <div className="flex-1">
                          <div className="text-xs font-semibold text-gray-600 uppercase mb-0.5">{field.label}</div>
                          <div className="text-sm font-bold text-black line-clamp-2">
                            {field.key === 'mortgageAmount' 
                              ? `$${parseInt(field.value).toLocaleString()}`
                              : field.key === 'ownershipType'
                              ? field.value.replace(/_/g, ' ').replace(/\b\w/g, (l: string) => l.toUpperCase())
                              : field.value
                            }
                          </div>
                        </div>
                      </label>
                    ))}
                  </div>
                </div>
              ))
            })()}
          </div>

          {/* Apply Button */}
          <div className="sticky bottom-0 bg-white border-t border-gray-200 p-4">
            <button
              onClick={() => {
                applySelectedEnrichments()
                setShowSidePanel(false)
              }}
              className="w-full px-4 py-3 bg-black text-white text-sm font-semibold rounded-lg hover:bg-gray-800 transition-colors"
            >
              Apply Selected Fields
            </button>
            <button
              onClick={() => setShowSidePanel(false)}
              className="w-full mt-2 px-4 py-2 text-sm text-gray-600 hover:text-black transition-colors"
            >
              Skip for now
            </button>
          </div>
        </div>
      )}

      {/* Main Content */}
      <div className={`${showSidePanel ? 'ml-80' : ''} transition-all duration-300`}>
        {/* Data Enrichment Toggle Button (when panel is closed but data is available) */}
        {!showSidePanel && fetchedData?.data && currentStep > 0 && (
          <button
            onClick={() => setShowSidePanel(true)}
            className="fixed left-4 top-24 z-20 px-4 py-3 bg-black text-white text-sm font-semibold rounded-lg shadow-lg hover:bg-gray-800 transition-all hover:scale-105"
            title="Open Data Enrichment Panel"
          >
            <span className="flex items-center gap-2">
              <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
              </svg>
              Auto-Fill Data
              {fetchedData?.fieldsCount > 0 && (
                <span className="ml-1 px-2 py-0.5 bg-white text-black text-xs font-bold rounded-full">
                  {fetchedData.fieldsCount}
                </span>
              )}
            </span>
          </button>
        )}

        {/* Progress Header with Branding */}
        <div className="sticky top-0 bg-white border-b border-gray-200 py-5 z-20">
          <div className="max-w-4xl mx-auto px-6">
            <div className="flex items-center justify-between mb-4">
              <div>
                <div className="flex items-center gap-3 mb-1">
                  <h2 className="text-base font-bold text-black">McKinney & Co</h2>
                  <span className="text-gray-300">|</span>
                  <span className="text-xs text-gray-600 uppercase tracking-wide">C-Store Insurance</span>
                </div>
                <p className="text-xs text-gray-500">
                  Step {currentStep} of {totalSteps}
                </p>
              </div>
              <div className="px-4 py-1.5 bg-black text-white text-xs font-semibold rounded-full">
                {Math.round(progress)}%
              </div>
            </div>
            <div className="relative h-1 bg-gray-200 rounded-full overflow-hidden">
              <div
                className="absolute top-0 left-0 h-full bg-black transition-all duration-300"
                style={{ width: `${progress}%` }}
              />
            </div>
            <div className="flex justify-between mt-5">
              {Array.from({ length: totalSteps }).map((_, i) => (
                <div key={i} className="flex flex-col items-center gap-1.5">
                  <div
                    className={`w-8 h-8 rounded-full flex items-center justify-center text-xs font-semibold transition-all ${
                      i < currentStep - 1
                        ? 'bg-black text-white'
                        : i === currentStep - 1
                        ? 'bg-black text-white'
                        : 'bg-gray-200 text-gray-400'
                    }`}
                  >
                    {i < currentStep - 1 ? '✓' : i + 1}
                  </div>
                  <span className={`text-[10px] font-medium ${i <= currentStep - 1 ? 'text-black' : 'text-gray-400'}`}>
                    {['Personal', 'Company', 'Property', 'Sales', 'Coverage', 'Business', 'Review'][i]}
                  </span>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Form Content */}
        <div className="max-w-4xl mx-auto p-4 md:p-6">
          <form onSubmit={handleSubmit(onSubmit)} className="space-y-6">
            
            {/* STEP 1: Personal Information (Lead Capture) */}
            {currentStep === 1 && (
              <div className="space-y-4">
                <div className="bg-white rounded-xl p-6">
                  <h3 className="text-lg font-bold text-black mb-6">Personal Information</h3>
                  
                  <div className="space-y-5">
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
                      <div>
                        <label className="block text-xs font-semibold text-gray-600 uppercase mb-2">Your Name</label>
                        <input
                          type="text"
                          {...register('contactName', { required: 'Name is required' })}
                          className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                          placeholder="John Doe"
                        />
                        {errors.contactName && (
                          <p className="mt-1.5 text-xs text-red-600">{errors.contactName.message}</p>
                        )}
                      </div>

                      <div>
                        <label className="block text-xs font-semibold text-gray-600 uppercase mb-2">Phone Number</label>
                        <input
                          type="tel"
                          {...register('contactNumber', { 
                            required: 'Phone number is required',
                            pattern: {
                              value: /^[\d\s\-\(\)]{10,}$/,
                              message: 'Please enter a valid 10-digit US phone number'
                            },
                            validate: (value) => {
                              const digits = value.replace(/\D/g, '')
                              if (digits.length !== 10) {
                                return 'Phone number must be exactly 10 digits'
                              }
                              return true
                            }
                          })}
                          className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                          placeholder="(555) 123-4567"
                        />
                        {errors.contactNumber && (
                          <p className="mt-1.5 text-xs text-red-600">{errors.contactNumber.message}</p>
                        )}
                      </div>
                    </div>

                    <div>
                      <label className="block text-xs font-semibold text-gray-600 uppercase mb-2">Email Address</label>
                      <input
                        type="email"
                        {...register('contactEmail', { 
                          required: 'Email is required',
                          pattern: {
                            value: /^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$/i,
                            message: 'Please enter a valid email address'
                          },
                          validate: (value) => {
                            if (!value || value.trim() === '') {
                              return 'Email cannot be empty'
                            }
                            return true
                          }
                        })}
                        className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                        placeholder="john@example.com"
                      />
                      {errors.contactEmail && (
                        <p className="mt-1.5 text-xs text-red-600">{errors.contactEmail.message}</p>
                      )}
                    </div>

                    <div>
                      <label className="block text-xs font-semibold text-gray-600 uppercase mb-2">Corporation Name</label>
                      <input
                        type="text"
                        {...register('corporationName')}
                        className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                        placeholder="ABC Corporation Inc."
                      />
                    </div>
                  </div>
                </div>
              </div>
            )}

            {/* STEP 2: Company Information */}
            {currentStep === 2 && (
              <div className="space-y-4">
                <div className="bg-white rounded-xl p-6">
                  <h3 className="text-lg font-bold text-black mb-6">Company Information</h3>
                  
                  <div className="space-y-5">
                    {/* Display Property Address and Corporation Name */}
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                      <div className="p-4 bg-gray-50 rounded-lg">
                        <label className="block text-xs font-semibold text-gray-600 uppercase mb-1">Property Address</label>
                        <p className="text-sm text-black font-medium">{formData.address || 'No address provided'}</p>
                      </div>
                      
                      <div className="p-4 bg-gray-50 rounded-lg">
                        <label className="block text-xs font-semibold text-gray-600 uppercase mb-1">Corporation Name</label>
                        <p className="text-sm text-black font-medium">{formData.corporationName || 'No corporation name provided'}</p>
                      </div>
                    </div>

                    <div>
                      <label className="block text-xs font-semibold text-gray-600 uppercase mb-2">Applicant Type</label>
                      <select
                        {...register('applicantType')}
                        className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                      >
                        <option value="">Select...</option>
                        <option value="individual">Individual</option>
                        <option value="partnership">Partnership</option>
                        <option value="corporation">Corporation</option>
                        <option value="joint_venture">Joint Venture</option>
                        <option value="llc">LLC</option>
                        <option value="other">Other</option>
                      </select>
                    </div>

                    <div>
                      <label className="block text-xs font-semibold text-gray-600 uppercase mb-2">DBA (Doing Business As)</label>
                      <input
                        type="text"
                        {...register('dba')}
                        className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                        placeholder="Business operating name"
                      />
                    </div>

                    <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
                      <div>
                        <label className="block text-xs font-semibold text-gray-600 uppercase mb-2">Proposed Effective Date</label>
                        <input
                          type="date"
                          {...register('proposedEffectiveDate')}
                          className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                        />
                      </div>

                      <div>
                        <label className="block text-xs font-semibold text-gray-600 uppercase mb-2">Target Premium</label>
                        <input
                          type="text"
                          {...register('targetPremium')}
                          placeholder="$5,000"
                          className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                        />
                      </div>
                    </div>

                    <div>
                      <label className="block text-xs font-semibold text-gray-600 uppercase mb-2">Prior Carrier</label>
                      <input
                        type="text"
                        {...register('priorCarrier')}
                        className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                        placeholder="Previous insurance provider"
                      />
                    </div>

                    <div>
                      <label className="block text-xs font-semibold text-gray-600 uppercase mb-2">
                        👤 Ownership Type
                      </label>
                      <select
                        {...register('ownershipType')}
                        className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                      >
                        <option value="">Select...</option>
                        <option value="owner">Owner</option>
                        <option value="tenant">Tenant</option>
                        <option value="lessors_risk">Lessor's Risk</option>
                        <option value="triple_net_lease">Triple Net Lease</option>
                      </select>
                    </div>

                    <div>
                      <label className="block text-xs font-semibold text-gray-600 uppercase mb-2">Operation Description</label>
                      <textarea
                        {...register('operationDescription')}
                        rows={4}
                        className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black resize-none"
                        placeholder="Describe your business operations..."
                      />
                    </div>
                  </div>
                </div>
              </div>
            )}

            {/* STEP 3: Property & Business Details */}
            {currentStep === 3 && (
              <div className="space-y-4">
                <div className="bg-white rounded-xl p-6">
                  <h3 className="text-lg font-bold text-black mb-6">Property & Business Details</h3>
                  
                  <div className="space-y-5">
                    {/* Business Operations */}
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
                      <div>
                        <label className="block text-xs font-semibold text-gray-600 uppercase mb-2">Hours of Operation</label>
                        <input
                          type="text"
                          {...register('hoursOfOperation')}
                          placeholder="e.g., 24/7 or 9AM-9PM"
                          className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                        />
                      </div>

                      <div>
                        <label className="block text-xs font-semibold text-gray-600 uppercase mb-2">No. of MPDs</label>
                        <input
                          type="text"
                          {...register('noOfMPDs')}
                          className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                        />
                      </div>
                    </div>

                    {/* Property Information */}
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
                      <div>
                        <label className="block text-xs font-semibold text-gray-600 uppercase mb-2">Construction Type</label>
                        <input
                          type="text"
                          {...register('constructionType')}
                          className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                        />
                      </div>

                      <div>
                        <label className="block text-xs font-semibold text-gray-600 uppercase mb-2">Total Sq. Footage</label>
                        <input
                          type="text"
                          {...register('totalSqFootage')}
                          className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                        />
                      </div>
                    </div>

                    {/* Building Years */}
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-5">
                      <div>
                        <label className="block text-xs font-semibold text-gray-600 uppercase mb-2">Year Built</label>
                        <input
                          type="text"
                          {...register('yearBuilt')}
                          className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                        />
                      </div>

                      <div>
                        <label className="block text-xs font-semibold text-gray-600 uppercase mb-2">Year of Latest Update</label>
                        <input
                          type="text"
                          {...register('yearOfLatestUpdate')}
                          className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                        />
                      </div>

                      <div>
                        <label className="block text-xs font-semibold text-gray-600 uppercase mb-2">Years Exp. in Business</label>
                        <input
                          type="text"
                          {...register('yearsInBusiness')}
                          className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                        />
                      </div>
                    </div>

                    <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
                      <div>
                        <label className="block text-xs font-semibold text-gray-600 uppercase mb-2">Years at this Location</label>
                        <input
                          type="text"
                          {...register('yearsAtLocation')}
                          className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                        />
                      </div>

                      <div>
                        <label className="block text-xs font-semibold text-gray-600 uppercase mb-2">Any Leased Out Space</label>
                        <input
                          type="text"
                          {...register('anyLeasedOutSpace')}
                          placeholder="Yes/No"
                          className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                        />
                      </div>
                    </div>

                    {/* Protection and Additional Info */}
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
                      <div>
                        <label className="block text-xs font-semibold text-gray-600 uppercase mb-2">Protection Class</label>
                        <input
                          type="text"
                          {...register('protectionClass')}
                          className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                        />
                      </div>

                      <div>
                        <label className="block text-xs font-semibold text-gray-600 uppercase mb-2">Additional Insured</label>
                        <input
                          type="text"
                          {...register('additionalInsured')}
                          className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                        />
                      </div>
                    </div>

                    {/* Alarm Systems */}
                    <div className="space-y-4 pt-3">
                      <label className="block text-sm font-bold text-black uppercase">Alarm Systems</label>
                      
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                        {/* Burglar Alarm */}
                        <div>
                          <label className="block text-xs font-semibold text-gray-600 uppercase mb-3">Burglar Alarm</label>
                          <div className="space-y-2.5">
                            <label className="flex items-center gap-2.5 cursor-pointer">
                              <input
                                type="checkbox"
                                {...register('burglarAlarmCentral')}
                                className="w-5 h-5 border-2 border-gray-300 rounded"
                              />
                              <span className="text-sm text-black">Central Station</span>
                            </label>
                            <label className="flex items-center gap-2.5 cursor-pointer">
                              <input
                                type="checkbox"
                                {...register('burglarAlarmLocal')}
                                className="w-5 h-5 border-2 border-gray-300 rounded"
                              />
                              <span className="text-sm text-black">Local</span>
                            </label>
                          </div>
                        </div>

                        {/* Fire Alarm */}
                        <div>
                          <label className="block text-xs font-semibold text-gray-600 uppercase mb-3">Fire Alarm</label>
                          <div className="space-y-2.5">
                            <label className="flex items-center gap-2.5 cursor-pointer">
                              <input
                                type="checkbox"
                                {...register('fireAlarmCentral')}
                                className="w-5 h-5 border-2 border-gray-300 rounded"
                              />
                              <span className="text-sm text-black">Central Station</span>
                            </label>
                            <label className="flex items-center gap-2.5 cursor-pointer">
                              <input
                                type="checkbox"
                                {...register('fireAlarmLocal')}
                                className="w-5 h-5 border-2 border-gray-300 rounded"
                              />
                              <span className="text-sm text-black">Local</span>
                            </label>
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            )}

            {/* STEP 4: Sales Data */}
            {currentStep === 4 && (
              <div className="space-y-4">
                <div className="bg-white rounded-xl p-6">
                  <h3 className="text-lg font-bold text-black mb-6">Sales Data</h3>
                  
                  <div className="space-y-6">
                    {/* Inside Sales */}
                    <div>
                      <label className="block text-xs font-semibold text-gray-600 uppercase mb-3">Inside Sales Total</label>
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <input
                          type="text"
                          {...register('insideSalesMonthly')}
                          placeholder="Monthly $"
                          className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                        />
                        <input
                          type="text"
                          {...register('insideSalesYearly')}
                          placeholder="Yearly $"
                          className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                        />
                      </div>
                    </div>

                    {/* Liquor Sales */}
                    <div>
                      <label className="block text-xs font-semibold text-gray-600 uppercase mb-3">Liquor Sales</label>
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <input
                          type="text"
                          {...register('liquorSalesMonthly')}
                          placeholder="Monthly $"
                          className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                        />
                        <input
                          type="text"
                          {...register('liquorSalesYearly')}
                          placeholder="Yearly $"
                          className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                        />
                      </div>
                    </div>

                    {/* Gasoline Sales */}
                    <div>
                      <label className="block text-xs font-semibold text-gray-600 uppercase mb-3">Gasoline Sales</label>
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <input
                          type="text"
                          {...register('gasolineSalesMonthly')}
                          placeholder="Monthly $"
                          className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                        />
                        <input
                          type="text"
                          {...register('gasolineSalesYearly')}
                          placeholder="Yearly $"
                          className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                        />
                      </div>
                    </div>

                    {/* Propane */}
                    <div>
                      <label className="block text-xs font-semibold text-gray-600 uppercase mb-3">Propane Filling/Exchange</label>
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <input
                          type="text"
                          {...register('propaneFillingExchangeMonthly')}
                          placeholder="Monthly $"
                          className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                        />
                        <input
                          type="text"
                          {...register('propaneFillingExchangeYearly')}
                          placeholder="Yearly $"
                          className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                        />
                      </div>
                    </div>

                    {/* Carwash */}
                    <div>
                      <label className="block text-xs font-semibold text-gray-600 uppercase mb-3">Carwash</label>
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <input
                          type="text"
                          {...register('carwashMonthly')}
                          placeholder="Monthly $"
                          className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                        />
                        <input
                          type="text"
                          {...register('carwashYearly')}
                          placeholder="Yearly $"
                          className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                        />
                      </div>
                    </div>

                    {/* Cooking */}
                    <div>
                      <label className="block text-xs font-semibold text-gray-600 uppercase mb-3">Cooking</label>
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <input
                          type="text"
                          {...register('cookingMonthly')}
                          placeholder="Monthly $"
                          className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                        />
                        <input
                          type="text"
                          {...register('cookingYearly')}
                          placeholder="Yearly $"
                          className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                        />
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            )}

            {/* STEP 5: Property Coverage */}
            {currentStep === 5 && (
              <div className="space-y-4">
                <div className="bg-white rounded-xl p-6">
                  <h3 className="text-lg font-bold text-black mb-6">Property Coverage</h3>
                  
                  <div className="space-y-5">
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
                      <div>
                        <label className="block text-xs font-semibold text-gray-600 uppercase mb-2">Building</label>
                        <input
                          type="text"
                          {...register('building')}
                          placeholder="$"
                          className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                        />
                      </div>

                      <div>
                        <label className="block text-xs font-semibold text-gray-600 uppercase mb-2">BPP</label>
                        <input
                          type="text"
                          {...register('bpp')}
                          placeholder="$"
                          className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                        />
                      </div>
                    </div>

                    <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
                      <div>
                        <label className="block text-xs font-semibold text-gray-600 uppercase mb-2">BI (Business Income)</label>
                        <input
                          type="text"
                          {...register('bi')}
                          placeholder="$"
                          className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                        />
                      </div>

                      <div>
                        <label className="block text-xs font-semibold text-gray-600 uppercase mb-2">Canopy</label>
                        <input
                          type="text"
                          {...register('canopy')}
                          placeholder="$"
                          className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                        />
                      </div>
                    </div>

                    <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
                      <div>
                        <label className="block text-xs font-semibold text-gray-600 uppercase mb-2">Pumps</label>
                        <input
                          type="text"
                          {...register('pumps')}
                          placeholder="$"
                          className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                        />
                      </div>

                      <div>
                        <label className="block text-xs font-semibold text-gray-600 uppercase mb-2">M&S (Mechanical & Electrical)</label>
                        <input
                          type="text"
                          {...register('ms')}
                          placeholder="$"
                          className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                        />
                      </div>
                    </div>

                    <div>
                      <label className="block text-xs font-semibold text-gray-600 uppercase mb-2">Coverage Limits</label>
                      <input
                        type="text"
                        {...register('coverageLimits')}
                        className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                      />
                    </div>
                  </div>
                </div>
              </div>
            )}

            {/* STEP 6: Business Details */}
            {currentStep === 6 && (
              <div className="space-y-4">
                <div className="bg-white rounded-xl p-6">
                  <h3 className="text-lg font-bold text-black mb-6">Business Details</h3>
                  
                  <div className="space-y-5">
                    <div>
                      <label className="block text-xs font-semibold text-gray-600 uppercase mb-2">FEIN</label>
                      <input
                        type="text"
                        {...register('fein')}
                        placeholder="Federal Employer ID Number"
                        className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                      />
                    </div>

                    <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
                      <div>
                        <label className="block text-xs font-semibold text-gray-600 uppercase mb-2">No. of Employees</label>
                        <input
                          type="text"
                          {...register('noOfEmployees')}
                          className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                        />
                      </div>

                      <div>
                        <label className="block text-xs font-semibold text-gray-600 uppercase mb-2">Payroll</label>
                        <input
                          type="text"
                          {...register('payroll')}
                          placeholder="$"
                          className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                        />
                      </div>
                    </div>

                    <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
                      <div>
                        <label className="block text-xs font-semibold text-gray-600 uppercase mb-2">Officers Incl/Excl</label>
                        <select
                          {...register('officersInclExcl')}
                          className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                        >
                          <option value="">Select...</option>
                          <option value="included">Included</option>
                          <option value="excluded">Excluded</option>
                        </select>
                      </div>

                      <div>
                        <label className="block text-xs font-semibold text-gray-600 uppercase mb-2">Ownership %</label>
                        <input
                          type="text"
                          {...register('ownership')}
                          placeholder="%"
                          className="w-full px-4 py-3 text-sm bg-gray-50 border-b-2 border-gray-300 focus:outline-none focus:border-black focus:bg-white transition-colors text-black"
                        />
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            )}

            {/* STEP 7: Review */}
            {currentStep === 7 && (
              <div className="space-y-4">
                <div className="bg-white rounded-xl p-6">
                  <h3 className="text-lg font-bold text-black mb-6">Review Your Application</h3>
                  
                  <div className="space-y-4">
                    {/* Personal Information */}
                    <div className="p-5 bg-gray-50 rounded-lg">
                      <h4 className="text-sm font-bold text-black mb-3 uppercase">Personal Information</h4>
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-3 text-sm text-gray-700">
                        {formData.contactName && <p><span className="font-semibold text-black">Contact Name:</span> {formData.contactName}</p>}
                        {formData.contactNumber && <p><span className="font-semibold text-black">Contact Number:</span> {formData.contactNumber}</p>}
                        {formData.contactEmail && <p><span className="font-semibold text-black">Email:</span> {formData.contactEmail}</p>}
                      </div>
                    </div>

                    {/* Company Information */}
                    <div className="p-5 bg-gray-50 rounded-lg">
                      <h4 className="text-sm font-bold text-black mb-3 uppercase">Company Information</h4>
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-3 text-sm text-gray-700">
                        {formData.address && <p className="md:col-span-2"><span className="font-semibold text-black">Property Address:</span> {formData.address}</p>}
                        {formData.applicantType && <p><span className="font-semibold text-black">Applicant Type:</span> {formData.applicantType}</p>}
                        {formData.corporationName && <p><span className="font-semibold text-black">Corporation Name:</span> {formData.corporationName}</p>}
                        {formData.dba && <p><span className="font-semibold text-black">DBA:</span> {formData.dba}</p>}
                        {formData.proposedEffectiveDate && <p><span className="font-semibold text-black">Effective Date:</span> {formData.proposedEffectiveDate}</p>}
                        {formData.ownershipType && <p><span className="font-semibold text-black">Ownership Type:</span> {formData.ownershipType}</p>}
                        {formData.operationDescription && <p className="md:col-span-2"><span className="font-semibold text-black">Operation Description:</span> {formData.operationDescription}</p>}
                      </div>
                    </div>

                    {/* Property & Business Details */}
                    <div className="p-5 bg-gray-50 rounded-lg">
                      <h4 className="text-sm font-bold text-black mb-3 uppercase">Property & Business Details</h4>
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-3 text-sm text-gray-700">
                        {formData.hoursOfOperation && <p><span className="font-semibold text-black">Hours of Operation:</span> {formData.hoursOfOperation}</p>}
                        {formData.noOfMPDs && <p><span className="font-semibold text-black">Number of MPDs:</span> {formData.noOfMPDs}</p>}
                        {formData.constructionType && <p><span className="font-semibold text-black">Construction Type:</span> {formData.constructionType}</p>}
                        {formData.totalSqFootage && <p><span className="font-semibold text-black">Total Square Footage:</span> {formData.totalSqFootage}</p>}
                        {formData.yearBuilt && <p><span className="font-semibold text-black">Year Built:</span> {formData.yearBuilt}</p>}
                        {formData.yearOfLatestUpdate && <p><span className="font-semibold text-black">Year of Latest Update:</span> {formData.yearOfLatestUpdate}</p>}
                        {formData.yearsInBusiness && <p><span className="font-semibold text-black">Years in Business:</span> {formData.yearsInBusiness}</p>}
                        {formData.yearsAtLocation && <p><span className="font-semibold text-black">Years in Current Location:</span> {formData.yearsAtLocation}</p>}
                        {formData.anyLeasedOutSpace && <p><span className="font-semibold text-black">Any Leased Out Space:</span> {formData.anyLeasedOutSpace}</p>}
                        {formData.protectionClass && <p><span className="font-semibold text-black">Protection Class:</span> {formData.protectionClass}</p>}
                        {formData.additionalInsured && <p><span className="font-semibold text-black">Additional Insured:</span> {formData.additionalInsured}</p>}
                        {(formData.burglarAlarmCentral || formData.burglarAlarmLocal) && (
                          <p><span className="font-semibold text-black">Burglar Alarm:</span> {
                            formData.burglarAlarmCentral && formData.burglarAlarmLocal ? 'Central Station, Local' :
                            formData.burglarAlarmCentral ? 'Central Station' :
                            'Local'
                          }</p>
                        )}
                        {(formData.fireAlarmCentral || formData.fireAlarmLocal) && (
                          <p><span className="font-semibold text-black">Fire Alarm:</span> {
                            formData.fireAlarmCentral && formData.fireAlarmLocal ? 'Central Station, Local' :
                            formData.fireAlarmCentral ? 'Central Station' :
                            'Local'
                          }</p>
                        )}
                      </div>
                    </div>

                    {/* Sales Data */}
                    <div className="p-5 bg-gray-50 rounded-lg">
                      <h4 className="text-sm font-bold text-black mb-3 uppercase">Sales Data</h4>
                      <div className="space-y-2 text-sm text-gray-700">
                        {(formData.insideSalesMonthly || formData.insideSalesYearly) && (
                          <div>
                            <p className="font-semibold text-black">Inside Sales:</p>
                            <div className="ml-4">
                              {formData.insideSalesMonthly && <p>Monthly: ${formData.insideSalesMonthly}</p>}
                              {formData.insideSalesYearly && <p>Yearly: ${formData.insideSalesYearly}</p>}
                            </div>
                          </div>
                        )}
                        {(formData.liquorSalesMonthly || formData.liquorSalesYearly) && (
                          <div>
                            <p className="font-semibold text-black">Liquor Sales:</p>
                            <div className="ml-4">
                              {formData.liquorSalesMonthly && <p>Monthly: ${formData.liquorSalesMonthly}</p>}
                              {formData.liquorSalesYearly && <p>Yearly: ${formData.liquorSalesYearly}</p>}
                            </div>
                          </div>
                        )}
                        {(formData.gasolineSalesMonthly || formData.gasolineSalesYearly) && (
                          <div>
                            <p className="font-semibold text-black">Gasoline Sales:</p>
                            <div className="ml-4">
                              {formData.gasolineSalesMonthly && <p>Monthly: ${formData.gasolineSalesMonthly}</p>}
                              {formData.gasolineSalesYearly && <p>Yearly: ${formData.gasolineSalesYearly}</p>}
                            </div>
                          </div>
                        )}
                        {(formData.propaneFillingExchangeMonthly || formData.propaneFillingExchangeYearly) && (
                          <div>
                            <p className="font-semibold text-black">Propane Sales:</p>
                            <div className="ml-4">
                              {formData.propaneFillingExchangeMonthly && <p>Monthly: ${formData.propaneFillingExchangeMonthly}</p>}
                              {formData.propaneFillingExchangeYearly && <p>Yearly: ${formData.propaneFillingExchangeYearly}</p>}
                            </div>
                          </div>
                        )}
                        {(formData.carwashMonthly || formData.carwashYearly) && (
                          <div>
                            <p className="font-semibold text-black">Car Wash Sales:</p>
                            <div className="ml-4">
                              {formData.carwashMonthly && <p>Monthly: ${formData.carwashMonthly}</p>}
                              {formData.carwashYearly && <p>Yearly: ${formData.carwashYearly}</p>}
                            </div>
                          </div>
                        )}
                        {(formData.cookingMonthly || formData.cookingYearly) && (
                          <div>
                            <p className="font-semibold text-black">Cooking Sales:</p>
                            <div className="ml-4">
                              {formData.cookingMonthly && <p>Monthly: ${formData.cookingMonthly}</p>}
                              {formData.cookingYearly && <p>Yearly: ${formData.cookingYearly}</p>}
                            </div>
                          </div>
                        )}
                      </div>
                    </div>

                    {/* Property Coverage */}
                    <div className="p-5 bg-gray-50 rounded-lg">
                      <h4 className="text-sm font-bold text-black mb-3 uppercase">Property Coverage</h4>
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-3 text-sm text-gray-700">
                        {formData.building && <p><span className="font-semibold text-black">Building:</span> ${formData.building}</p>}
                        {formData.bpp && <p><span className="font-semibold text-black">BPP:</span> ${formData.bpp}</p>}
                        {formData.bi && <p><span className="font-semibold text-black">Business Income:</span> ${formData.bi}</p>}
                        {formData.canopy && <p><span className="font-semibold text-black">Canopy:</span> ${formData.canopy}</p>}
                        {formData.pumps && <p><span className="font-semibold text-black">Pumps:</span> ${formData.pumps}</p>}
                        {formData.ms && <p><span className="font-semibold text-black">M&S:</span> ${formData.ms}</p>}
                      </div>
                    </div>

                    {/* Business Details */}
                    <div className="p-5 bg-gray-50 rounded-lg">
                      <h4 className="text-sm font-bold text-black mb-3 uppercase">Business Details</h4>
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-3 text-sm text-gray-700">
                        {formData.fein && <p><span className="font-semibold text-black">FEIN:</span> {formData.fein}</p>}
                        {formData.noOfEmployees && <p><span className="font-semibold text-black">No. of Employees:</span> {formData.noOfEmployees}</p>}
                        {formData.payroll && <p><span className="font-semibold text-black">Payroll:</span> ${formData.payroll}</p>}
                        {formData.officersInclExcl && <p><span className="font-semibold text-black">Officers Incl/Excl:</span> {formData.officersInclExcl}</p>}
                        {formData.ownership && <p><span className="font-semibold text-black">Ownership %:</span> {formData.ownership}%</p>}
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            )}

            {/* Navigation Buttons */}
            <div className="flex gap-3 pt-6 pb-8">
              {currentStep > 1 && (
                <button
                  type="button"
                  onClick={prevStep}
                  className="px-6 py-3 bg-white text-black text-sm font-semibold border border-gray-300 rounded-lg hover:bg-gray-50 transition-colors"
                >
                  ← Back
                </button>
              )}
              
              <button
                type="button"
                onClick={handleSaveAndExit}
                className="px-6 py-3 bg-white text-gray-700 text-sm font-semibold border border-gray-300 rounded-lg hover:bg-gray-50 transition-colors"
                title="Save your progress and exit"
              >
                💾 Save & Exit
              </button>
              
              {currentStep < totalSteps ? (
                <button
                  type="button"
                  onClick={nextStep}
                  className="flex-1 px-6 py-3 bg-black text-white text-sm font-semibold rounded-lg hover:bg-gray-800 transition-colors"
                >
                  Continue →
                </button>
              ) : (
                <button
                  type="submit"
                  className="flex-1 px-6 py-3 bg-black text-white text-sm font-semibold rounded-lg hover:bg-gray-800 transition-colors"
                >
                  Submit Application
                </button>
              )}
            </div>
          </form>
        </div>
      </div>

      <style jsx>{`
        @keyframes shimmer {
          0% {
            transform: translateX(-100%);
          }
          100% {
            transform: translateX(100%);
          }
        }
        .animate-shimmer {
          animation: shimmer 2s infinite;
        }
      `}</style>
    </div>
  )
}
