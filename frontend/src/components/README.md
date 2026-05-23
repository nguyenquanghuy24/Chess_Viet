# Authentication Components

## Login Component (`Login.tsx`)
- Email and password input fields
- Password visibility toggle
- Form validation
- Error handling
- Login API integration with `/auth/login` endpoint
- Stores token and user data in localStorage

## Register Component (`Register.tsx`)
- Username, email, and password fields
- Password confirmation with visibility toggle
- Form validation (password match, minimum length)
- Error handling
- Registration API integration with `/auth/register` endpoint
- Stores token and user data in localStorage

## Features
- Tab-based interface to switch between Login and Register
- Consistent styling with the existing chess app
- Demo login option still available for testing
- Responsive design
- Input validation with helpful error messages
- Password visibility toggles for better UX

## Styling
- Added comprehensive CSS for auth components in `styles.css`
- Dark theme matching the existing app
- Form inputs with focus states
- Error message styling
- Smooth transitions and hover effects

## Integration
- Components are imported in `main.tsx`
- Auth state managed in the main App component
- Toggle between login/register modes
- Maintains existing demo login functionality