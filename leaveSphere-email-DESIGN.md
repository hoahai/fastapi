---
name: Lumina Email
colors:
  surface: '#f9f9fb'
  surface-dim: '#d9dadc'
  surface-bright: '#f9f9fb'
  surface-container-lowest: '#ffffff'
  surface-container-low: '#f3f3f5'
  surface-container: '#edeef0'
  surface-container-high: '#e8e8ea'
  surface-container-highest: '#e2e2e4'
  on-surface: '#1a1c1d'
  on-surface-variant: '#494551'
  inverse-surface: '#2f3132'
  inverse-on-surface: '#f0f0f2'
  outline: '#7a7582'
  outline-variant: '#cbc4d3'
  surface-tint: '#684ea7'
  primary: '#15003d'
  on-primary: '#ffffff'
  primary-container: '#2d0a6a'
  on-primary-container: '#977cd9'
  inverse-primary: '#d0bcff'
  secondary: '#632ce5'
  on-secondary: '#ffffff'
  secondary-container: '#7c4dff'
  on-secondary-container: '#fcf6ff'
  tertiary: '#210800'
  on-tertiary: '#ffffff'
  tertiary-container: '#431700'
  on-tertiary-container: '#c07b58'
  error: '#ba1a1a'
  on-error: '#ffffff'
  error-container: '#ffdad6'
  on-error-container: '#93000a'
  primary-fixed: '#e9ddff'
  primary-fixed-dim: '#d0bcff'
  on-primary-fixed: '#23005c'
  on-primary-fixed-variant: '#50358e'
  secondary-fixed: '#e8deff'
  secondary-fixed-dim: '#cdbdff'
  on-secondary-fixed: '#20005f'
  on-secondary-fixed-variant: '#4f00d0'
  tertiary-fixed: '#ffdbcb'
  tertiary-fixed-dim: '#ffb693'
  on-tertiary-fixed: '#351000'
  on-tertiary-fixed-variant: '#6e381b'
  background: '#f9f9fb'
  on-background: '#1a1c1d'
  surface-variant: '#e2e2e4'
typography:
  headline-lg:
    fontFamily: Manrope
    fontSize: 30px
    fontWeight: '700'
    lineHeight: 38px
    letterSpacing: -0.02em
  headline-md:
    fontFamily: Manrope
    fontSize: 20px
    fontWeight: '600'
    lineHeight: 28px
  body-lg:
    fontFamily: Inter
    fontSize: 16px
    fontWeight: '400'
    lineHeight: 24px
  body-sm:
    fontFamily: Inter
    fontSize: 14px
    fontWeight: '400'
    lineHeight: 20px
  label-bold:
    fontFamily: Inter
    fontSize: 12px
    fontWeight: '700'
    lineHeight: 16px
    letterSpacing: 0.05em
  headline-lg-mobile:
    fontFamily: Manrope
    fontSize: 24px
    fontWeight: '700'
    lineHeight: 32px
rounded:
  sm: 0.125rem
  DEFAULT: 0.25rem
  md: 0.375rem
  lg: 0.5rem
  xl: 0.75rem
  full: 9999px
spacing:
  container-max: 600px
  margin-x: 32px
  stack-sm: 8px
  stack-md: 16px
  stack-lg: 32px
  gutter: 12px
---

## Brand & Style
The design system for this PTO submission app is rooted in a **Corporate / Modern** aesthetic. It prioritizes clarity, efficiency, and institutional trust—essential for HR-related communications. The personality is professional yet accessible, ensuring that employees feel both informed and valued. 

Visuals are defined by high-contrast typography, generous whitespace to reduce cognitive load, and a refined color palette derived from deep plums and crisp whites. The system avoids unnecessary decorative flourishes, focusing instead on structured data presentation and unmistakable calls to action.

## Colors
The palette is anchored by a deep purple (`#2D0A6A`), serving as the primary brand identifier for headers and critical actions. A secondary, more vibrant violet is used for interactive elements and highlights. 

- **Primary:** Used for headers, hero sections, and primary buttons.
- **Secondary:** Used for hover states and secondary emphasis.
- **Neutral:** A range of cool grays and off-whites (`#F9F9FB`) provide the canvas for content, ensuring the interface feels airy and organized.
- **Semantic:** Standardized success (green) and pending (amber) tones are utilized for status indicators within PTO request tables.

## Typography
This design system employs a dual-font strategy. **Manrope** is used for headings to provide a modern, structural feel with its geometric proportions. **Inter** is used for all body copy and data tables due to its exceptional legibility at small sizes and its neutral, utilitarian tone.

Text hierarchy is strictly enforced. Primary headings use a tight letter-spacing for a sophisticated look, while labels and data headers utilize uppercase styling with increased tracking to differentiate them from prose.

## Layout & Spacing
The layout follows a **Fixed Grid** model optimized for email clients, centered with a maximum width of 600px. This ensures a consistent reading experience across Outlook, Gmail, and Apple Mail.

Spacing follows an 8px base rhythm. Large vertical gaps (`stack-lg`) separate major sections like the header, the request details, and the footer. Data tables use a compact `gutter` to keep information dense but readable. On mobile, horizontal margins scale down from 32px to 20px to maximize screen real estate.

## Elevation & Depth
This design system utilizes **Tonal Layers** rather than heavy shadows to maintain a clean, "flat-plus" appearance. 

- **Base Surface:** The main email background is a soft neutral (`#F9F9FB`).
- **Content Cards:** The primary message area is pure white with a subtle, 1px light-gray border.
- **In-set Sections:** Data tables for PTO details are housed in light-gray containers or use alternating row stripes to create depth without relying on elevation effects. 
- **Shadows:** When necessary for primary buttons, a very soft, low-opacity (10%) ambient shadow is applied to suggest interactivity.

## Shapes
The shape language is **Soft**. A consistent 4px (`0.25rem`) radius is applied to buttons, data containers, and status chips. This subtle rounding softens the corporate edge of the design, making the app feel modern and approachable while maintaining a professional silhouette. Larger components like the main container card may use `rounded-lg` (8px) for a more pronounced frame.

## Components
- **Primary Buttons:** Solid `#2D0A6A` background with white text. Padding is 12px vertical and 24px horizontal. Always centered or full-width on mobile.
- **Data Tables:** Used for PTO request details (Dates, Type, Hours). Labels are `label-bold` in a muted gray; values are `body-lg` in near-black.
- **Status Chips:** Small, rounded indicators for "Approved", "Pending", or "Denied". They use high-chroma text on a 10% opacity background of the same color.
- **Email Header:** A full-width primary color block containing the white logo, providing a strong visual anchor.
- **Information Banners:** Light tinted boxes used for "Next Steps" or manager notes, utilizing a 1px border of the same tint to distinguish from the main body.
- **Dividers:** Minimal, 1px solid lines using a light neutral color to separate the footer from the main content.