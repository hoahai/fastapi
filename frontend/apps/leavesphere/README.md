# LeaveSphere Frontend

This document describes the current LeaveSphere frontend behavior and rules as implemented in:

- `frontend/apps/leavesphere/src/lib/ptoRequestActionConfig.ts`
- `frontend/apps/leavesphere/src/components/PtoRequestDetailModal.tsx`
- `frontend/apps/leavesphere/src/pages/MyPtoPage.tsx`
- `frontend/apps/leavesphere/src/pages/LeaveManagementPage.tsx`

## Main Pages

- `MyPtoPage`:
  - normal user PTO requests
  - manager review area for direct reports
- `LeaveManagementPage`:
  - Leave Management calendar, requests, balances, setup

## Role + Status Permission Rules

Source of truth: `getPtoRequestActionConfig(...)`.

### Roles

- `user`
- `approver` (manager)
- `admin`

### Status normalization

- data uses `cancelled`
- permission table keys use `canceled`
- config normalizes `cancelled -> canceled`

### Date boundary

- `before start date`: `today < startDate`
- `on/after start date`: `today >= startDate`

### Action flags

- `canEditForm`
- `canSubmit`
- `canCancel`
- `canApprove`
- `canReject`
- `canRevert`

### Before start date

- `user`
  - `create`: edit + submit
  - `pending`: edit + submit + cancel
  - `approved/rejected/canceled`: no actions
- `approver`
  - `pending`: approve + reject + cancel
  - `approved`: reject + cancel + revert
  - `rejected`: approve + cancel + revert
  - `canceled`: approve + reject + revert
- `admin`
  - `create`: edit + submit
  - `pending`: edit + submit + approve + reject + cancel
  - `approved`: edit + submit + reject + cancel + revert
  - `rejected`: approve + cancel + revert
  - `canceled`: revert

### On/after start date

- `user`
  - `create`: edit + submit
  - `pending/approved/rejected/canceled`: no actions
- `approver`
  - `pending`: approve + reject
  - `approved/rejected/canceled`: no actions
- `admin`
  - `create`: edit + submit
  - `pending`: edit + submit + approve + reject + cancel
  - `approved`: edit + submit + reject + cancel
  - `rejected`: approve + cancel
  - `canceled`: revert

## PTO Detail Modal Behavior

Source: `PtoRequestDetailModal`.

- Modal tracks two dirty sources:
  - core form dirty (`form` vs `baselineForm`)
  - optional `externalDirty` from page-level fields (admin/manager note)
- Unsaved changes behavior:
  - block outside click / Escape / close when dirty
  - show shared `UnsavedChangesDialog`
- Save button behavior:
  - shown only when `submitHandler` exists and (`canSave` or `saving`)
  - `canSave = valid core fields + hasUnsavedChanges`
- Save callback contract:
  - `onSave/onSubmit` may return `false` to cancel close
  - used for confirmation flows

### Hours auto-calculate behavior

- On open:
  - auto-calculate only when `mode=create` OR initial `hours` is empty
- On edit with existing hours:
  - no auto-calc at first open
- If date range changes after open:
  - auto-calculate runs again

## Note Behavior (Manager/Admin)

### Manager note in `MyPtoPage` review modal

- editable only when selected request status is `pending` and action config allows review actions
- note-only edits do not show `Save changes` (manager review uses decision buttons for note persistence)
- note is saved through `Approve/Reject/Cancel` actions

### Admin note in `LeaveManagementPage` detail modal

- editable only when selected request status is `pending` and action config allows review actions
- `Save changes` updates request body fields (`type/startDate/endDate/hours/reason`) only
- `Save changes` does not persist note
- when note is dirty and admin clicks `Save changes`, show confirmation dialog:
  - title: `Admin note won't be saved`
  - confirm: `Save details only`
  - cancel: `Go back`

## Decision Action Confirmations

Shared confirmation component:

- `frontend/apps/tradsphere/src/components/ui/confirm-dialog.tsx`

Applied in both:

- `MyPtoPage` manager review actions
- `LeaveManagementPage` admin review actions

Confirmed actions:

- Approve
- Reject
- Cancel
- Revert

## Calendar Behavior

### My PTO calendar (`MyPtoPage`)

- shows user requests and team-region holidays

### Leave Management calendar (`LeaveManagementPage`)

- now shows all request statuses (pending, approved, rejected, cancelled) plus holidays
- status chip tone is mapped via `mapLeaveSpherePtoStatusToChipTone`

## Current Style Conventions

- request-detail summary card uses neutral slate background with status chip at top-right
- long summary values (employee/manager in Leave Management detail) use truncation + tooltip title attribute
- Approve action button in review modals:
  - secondary (`outline`) style
  - green border/text (`border-emerald-300 text-emerald-700 hover:bg-emerald-50`)
- dialog/confirm flows use app-styled dialog components, not browser-native `alert/confirm`
