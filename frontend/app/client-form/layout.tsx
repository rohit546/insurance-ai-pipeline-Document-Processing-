import '@/styles/client-form.css'

export default function ClientFormLayout({
  children,
}: {
  children: React.ReactNode
}) {
  // Google Maps script is already loaded in root layout
  // Just return children with CSS imported
  return <>{children}</>
}
