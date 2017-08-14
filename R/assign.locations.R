#' \code{assign.locations} package
#'
#' assign.locations
#'
#' See the Vignette in the future
#'
#' @docType package
#' @name assign.locations
#' @importFrom dplyr %>% select
NULL

## quiets concerns of R CMD check re: the .'s that appear in pipelines
if(getRversion() >= "2.15.1")  utils::globalVariables(c("."))

#' @title funImport
#'
#' @description Assigns address to shapes
#' @param D raw data with all fields
#' @param source data source name
#' @param match.threshold string match quality
#' @param l.study.extent list with cities, zips, states
#' @param pkg.data.root path to dropbox pkg.data root
#' @keywords parcels, clean
#' @export
#' @import stringr
#'     data.table
#'     stringdist
#'     methods.string
#'     foreign
#'     parcels
#'     pkg.data.paths
funImport <- function(D, source='', match.threshold=0.6, l.study.extent=NULL,
                      pkg.data.root = NULL){
  pkg.name <- pkg.data.path <- address.id <- NULL
  # Build the substrate
  l.address <- fun.l.address(pkg.data.root)
  l <- list(); mgs.list <- list(); l$D <- D;
  l$source <- source; l$match.threshold<-match.threshold
  if (is.null(l.study.extent)){
    dt.pkg.data <- pkg.data.paths::paths(pkg.data.root, str.pkg.name = 'parcels')
    parcels.address <- parcels::address(dt.pkg.data$pkg.root[1])
    l.study.extent <- list(cities=unique(parcels.address$city), zips = unique(parcels.address$zip),
                           states=unique(parcels.address$state))
  }
  if (is.null(pkg.data.root)) stop(paste('No pkg.data.root defined'))
  # Generate checklis
  check <- funImport.check.list(l$D)
  # Break clean if any of these condtions are true
  if (check$location.id & l$source != 'parcels.address') stop('Location already assigned')
  if (check$data.id) stop('data.id automatically created. Please remove')
  if (source==''){stop('must define data source (example: mmed.denver') } else {l$D$record.source <- l$source}
  # Define record ids
  if (check$record.id==FALSE) l$D$record.id <- as.character(1:nrow(l$D))
  # Define address ids
  l$D <- funImport.address.ids(l$D)
  l$data <- funImport.data(l) # Retains record.id specific info
  l$address <- funImport.address(l$D, lookup.address=l.address$parcels) # Cleans and formats address for location assignment
  l$location <- funImport.locations(l, match.threshold, l.study.extent, pkg.data.path) # Assigns address to parcel, outline, point
  setkey(l$data, address.id)
  setkey(l$address, address.id)
  DT <- l$data[l$address]
  setkey(DT, address.id)
  setkey(l$location, address.id)
  DT <- DT[l$location, allow.cartesian=TRUE]
  c.head <- c('record.id', 'address.id', 'location.type', 'location.id', 'street', 'city', 'state', 'zip')
  c.remain <- names(DT)[!(names(DT)%in%c.head)]
  cols <- c(c.head, c.remain)
  DT <- DT[, (cols), with=FALSE]
  return(DT)
}
#' @title funImport.check.list
#'
#' @description checks to see what data fields are being used
#' @param x data.table
#' @keywords check, import, spatial
#' @export
#' @import stringr
#'     data.table
funImport.check.list <- function(x){
  pkg.name <- pkg.data.path <- address.id <- NULL
  l <- list()
  l$location.id <- ('location.id' %in% names(x))
  l$data.id <- ('data.id' %in% names(x))
  l$cityStateZip <- ('cityStateZip' %in% names(x))
  l$street <- ('street' %in% names(x))
  l$city <- ('city' %in% names(x))
  l$state <- ('state' %in% names(x))
  l$zip <- ('zip' %in% names(x))
  l$name <- ('name' %in% names(x))
  l$name.key <- ('name.key' %in% names(x))
  l$name.lic <- ('name.lic' %in% names(x))
  l$record.id <- ('record.id' %in% names(x))
  l$address <- length(grep('(?i)address(?!.id)', names(x), value=TRUE, perl=TRUE))>0
  return(l)
}
#' @title funImport.address
#'
#' @description Explodes and cleans address
#' @param x data.table
#' @param lookup.address data.table used to fill missing zips and cities usually parcel.address
#' @keywords check, import, spatial
#' @export
#' @import stringr
#'     data.table
#'     methods.string
#'     parcels
#'     stats
funImport.address <- function(x, lookup.address=NULL){
  city <- state <- zip <- geocode.id <- address.id <- address.cols <- NULL
  l <- na.omit <- address.id <- pkg.name <- NULL
  street.num.temp <- NULL
  regex.address <- '(?i)(^|.*.)address($|.*.)'
  regex.geo <- paste0('(?i)(^)(',paste0(c('city', 'state', 'zip', 'street', 'unit','cityStateZip'),collapse='|'),')($)|',regex.address)
  cols <- names(x)[str_detect(names(x), regex(regex.geo, perl=TRUE))]
  x <- x[,(cols),with=FALSE]
  x <- unique(x, by='address.id')
  # Formats for names from columns
  check <- funImport.check.list(x)
  # Build the street info
  if (check$street==TRUE){
    street <- explode.street(x$street)
    street$street <- NULL
    x <- data.table(x, street)
  } else {
    stop(paste('Error in funImport.address: No street for dataset', l$source))
  }
  # Try and fill city state zip
  if (check$city == FALSE|check$state==FALSE|check$zip==FALSE){
    if (check$cityStateZip==TRUE){
      cityStateZip <- methods.string::explode.cityStateZip(x)
      if (check$city==FALSE) x$city <- cityStateZip$city
      if (check$state==FALSE) x$state <- cityStateZip$state
      if (check$zip==FALSE) x$zip <- cityStateZip$zip
      x$cityStateZip <- NULL
      check <- funImport.check.list(x)
    } 
    if (check$cityStateZip==FALSE){
      if (check$city==FALSE & check$zip==FALSE) stop(paste('Error in funImport.address: No city or zip for dataset', l$source))
      if (check$city==FALSE) x$city <- ''
      if (check$zip==FALSE) x$zip <- ''
    }
    if (check$address==TRUE){ # What is address (think it is full 134 45th street, denver, co 402389)
      address.cols <- stats::na.omit(str_extract(names(x), regex(regex.address, perl=TRUE)))
      address.col <- names(which.max(lapply(x[,(address.cols),with=FALSE], function(y) length(na.omit(y)))))[1]
      address <- methods.string::explode.cityStateZip(x[, address.col, with=FALSE])
      if (check$city==FALSE) x$city <- address$city
      if (check$state==FALSE) x$state <- address$state
      if (check$zip==FALSE) x$zip <-address$zip
    }
    if(check$state==FALSE){
      dt.states <- methods.string::fill.missing.state(x)
      setkey(x, address.id)
      x <- dt.states[x]
    } 
    check <- funImport.check.list(x)
    #x <- try(methods.string::fill.missing.zip.city(x, lookup.address))
  }
  x$zip <- methods.string::extract.zip(x$zip)
  x$city <- methods.string::clean.city(x$city)
  if(check$state==FALSE & check$zip==TRUE){
    dt.states <- methods.string::fill.missing.state(x)
    setkey(x, address.id)
    x <- dt.states[x]
  }
  check <- funImport.check.list(x)
  if(check$state==TRUE){
    x$state <- methods.string::extract.state(x$state)
  }
  return(x)
}
#' @title funImport.data
#'
#' @description checks to see what data fields are being used
#' @param l list of data
#' @keywords check, import, spatial
#' @export
#' @import stringr
#'     data.table
#'     methods.string
funImport.data <- function(l){
  x <- l$D
  check <- funImport.check.list(x)
  regex.geo <- paste0('(?i)(^(',paste0(c('city', 'state', 'zip', 'unit', 'street'),collapse='|'),')($))')
  cols <- names(x)[!str_detect(names(x), regex(regex.geo, perl=TRUE))]
  x <- x[,(cols),with=FALSE]
  # name
  if (check$name.lic){
    x$name.lic <- clean.name(x$name.lic)
    x$name.lic.key <- clean.name.key(x$name.lic)
  } else {
    x$name.lic <- ''
    x$name.lic.key <- ''
  }  
  # name.key
  if (check$name){
    x$name <- clean.name(x$name)
    x$name.key <- clean.name.key(x$name)
  } else {
    x$name <- ''
    x$name.key <- ''
  }
  # Address.alts
  if (check$address){
    address.cols<-na.omit(str_extract(names(x), '(?i).*.address($|.*.)'))
    address.cols.new<- paste0(address.cols,'.alt')
    setnames(x, address.cols, address.cols.new)
  }
  return(x)
}
#' @title funImport.address.ids
#'
#' @description Creates unique address.ids for places that are different
#' @param x data.table
#' @keywords address.ids
#' @export
#' @import stringr
#'     data.table
funImport.address.ids <- function(x){
  address.id <- NULL
  regex.address <- '(?i)(^|.*.)address($|.*.)'
  regex.geo <- paste0('(?i)(^)(',paste0(c('city', 'state', 'zip', 'street', 'unit','cityStateZip'),collapse='|'),')($)|',regex.address)
  cols <- names(x)[str_detect(names(x), regex(regex.geo, perl=TRUE))]
  x <- x[,address.id:=.GRP, by=cols]
  return(x)
}
#' @title funImport.locations
#'
#' @description assigns address to locations (shapefiles)
#' @param l list with all location data.tables
#' @param match.threshold string matching criteria
#' @param l.study.extent cities, zips, states
#' @param pkg.data.root dropbox pkg root
#' @keywords assign, location, import
#' @export
#' @import stringr
#'     data.table
#'     methods.string
funImport.locations <- function(l, match.threshold, l.study.extent, pkg.data.root){
  file.name <- street <- zip <- city <- NULL
  source(pkg.data.paths::dt(pkg.data.root)[file.name=='api.key.R']$sys.path)
  api.key <- api.key()
  address <- l$address
  address.ids <- address$address.id
  if (!(is.null(l.study.extent))){
    address <- address[zip %in% l.study.extent$zips | city %in% l.study.extent$cities]
  }
  address <- address[!(is.na(street))]
  locs <- funUpdate.locs(address, match.threshold, pkg.data.root)
  locs <- funImport.location.geocode(locs, address, pkg.data.root, api.key, l.study.extent, l$source)
  locs <- funImport.location.intersect(locs, address, pkg.data.root, l$source)
  return(locs)
}
#' @title funImport.location.address
#'
#' @description string matches address (x) to shapefiles address
#' @param l.location points, parcels, geocodes.bad, outlines
#' @param x the address file (to be assigned to l.locations)
#' @keywords assign location import
#' @export
#' @import stringr
#'     data.table
#'     methods.string
#'     tidyr
funImport.location.address <- function(l.location, x){
  street.num.range <- street.num <- street.num.low <- street.num.hi <- NULL
  data.street.num <- data.street.direction <- street.direction.prefix <- NULL
  combn <- data.num.max <- address.id <- location.id <- DT.geo <- NULL
  match.score <- location.street.direction <- location.source <- NULL
  match.cols.n <- match.score <- location.street.direction <- NULL
  street.body <- max.score <- location.match.score <- street <- NULL
  i.comb <- location.street.num <- city <- state <- zip <- NULL
  geocode.id <- adddress.id <- street.num.temp <- NULL
  
  # data: costar.adddress, mmed, etc.
  # locations: parcel.address, point.address
  cat(paste('Assigning', l.location$type), sep='\n')
  data <- x # MMED
  if(!is.null(data$street.num.hi)) data$street.num <- NULL; 
  data <- data %>% # For roll join
    gather(street.num.range, street.num, street.num.low:street.num.hi)
  data <- as.data.table(data)
  data$street.num.range<-NULL
  data <- data[, data.street.num := street.num]
  data <- data[, data.street.direction := street.direction.prefix]
  setkey(data,NULL)
  data <- unique(data)  
  
  location <- l.location$address # Parcels, points, outlines
  match.threshold <- l.location$match.threshold
  type <- l.location$type
  if (any(str_detect(names(data), 'record.id'))) data$record.id<-NULL
  if (missing(match.threshold)|match.threshold>0.9) match.threshold <- 0.8
  # Clone fields for join
  location$location.street.num <- location$street.num
  location$location.street.direction <- location$street.direction.prefix
  # Create combinations of street fields for matching algo
  cols.fixed <- c('street.body', 'city', 'street.num', 'state')
  cols.var.list <- list(list('street.direction.prefix', 10),
                        list('street.type', 10),
                        list('street.direction.suffix', 6),
                        list('street.unit.type', 3),
                        list('street.unit', 3),
                        list('zip',10))
  cols.var.DT <- rbindlist(cols.var.list)
  setnames(cols.var.DT, names(cols.var.DT), c('var', 'score'))
  vec.m <- seq(nrow(cols.var.DT), 1, -1)
  comb.vars <- unlist(lapply(vec.m, combn, x = cols.var.DT$var, simplify=FALSE),recursive = FALSE)
  comb.scores <- sapply(unlist(lapply(vec.m, combn, x = cols.var.DT$score, simplify=FALSE),recursive = FALSE),sum)
  combs <- cbind(comb.vars,comb.scores)
  
  # Set match.threshold
  best.score <- max(unlist(combs[,2]))
  scores.pct <- comb.scores/best.score
  comb.scores.test <- scores.pct > match.threshold
  combs <- combs[comb.scores.test,]
  combs.n <- nrow(combs)
  
  DT.list <- list()
  start.time <- Sys.time()
  for(i.comb in 1:combs.n){
    cols.comb <- combs[i.comb,]$comb.vars
    score <- combs[i.comb,]$comb.scores
    cols <-c(cols.comb, cols.fixed)
    setkeyv(data, cols)
    setkeyv(location, cols)
    cols.data <- c('address.id', 'data.street.direction', cols)
    data.sub <- data[, (cols.data), with=FALSE]
    if (nrow(data.sub)>0){ # Added tghe on 10/7/16 for cases where there is no matches (single address queries), may be buggy
      cols.locations <- c('location.id', 'location.source', 'location.street.direction', 'location.street.num', cols)
      locations.sub <- location[, (cols.locations), with=FALSE]
      setkeyv(data.sub, cols)
      setkeyv(locations.sub, cols)
      data.sub <- data.sub[, data.num.max:=max(street.num), by=address.id]
      data.location <- data.sub[locations.sub, roll=Inf][!is.na(address.id) & location.street.num<=data.num.max]
      if (nrow(data.location)>0){
        setkey(data.location, address.id, location.id)
        data.location <- unique(data.location)
        data.location[ ,match.cols.n := length(cols)]
        data.location[ ,match.score := score]
        data.location[ ,i.comb := i.comb]
        DT.list[[i.comb]]<-data.location
      }
    }
  }
  end.time <- Sys.time()
  # print(end.time-start.time)
  DT <- rbindlist(DT.list, use.names=TRUE, fill=TRUE)
  if (nrow(DT)>0){
    setkey(DT, address.id, location.id)
    #DT[, location.street.num:=NULL]
    #DT[, location.street.direction:=NULL];
    DT$location.type <- type
    # Bad n/s match
    DT <- DT[data.street.direction != location.street.direction &
               (nchar(data.street.direction) > 0 & nchar(location.street.direction) > 0),
             match.score:=0]
    # Penalize geocoded by 1
    DT <- DT[grep('geocode', location.source), match.score := match.score-1]
    DT <- DT[grep('(?i)[0-9]box {1,}(?=$)', street.body, ignore.case = TRUE, perl=TRUE), match.score:=match.score+10]
    DT <- unique(DT[, max.score:=max(match.score),
                    by=address.id][match.score == max.score][,location.match.score:=round(match.score/best.score,2)])
    cols <- c('address.id', 'location.id', 'location.type', 'location.source')
    cols <- c(cols,
              names(DT)[!(names(DT) %in% c(cols, 'data.street.direction', 'match.cols.n','match.score', 'i.comb', 'max.score', 'location.type'))])
    DT <- DT[, (cols), with=FALSE]
  }
  # Add in missing if bad geocode (may happen if bad address and missing fields)
  if (l.location$type == "geocodes.bad") {
    geocodes.bad <- l.location$address[, .(street,city,state,zip,geocode.id=location.id, location.source)]
    setkey(geocodes.bad, street, city, state, zip)
    setkey(x, street, city, state, zip)
    DT.2 <- geocodes.bad[x]
    DT.2 <- DT.2[!is.na(geocode.id)]
    setnames(DT.2, 'geocode.id', 'location.id')
    DT.2 <- DT.2[is.na(street.num), data.num.max:=0]
    DT.2$street.num.temp <- as.integer(0)
    DT.2 <- DT.2[!is.na(street.num), street.num.temp:=as.integer(street.num)]
    DT.2 <- DT.2[, data.num.max:=as.numeric(max(street.num.temp)), by=address.id]
    DT.2$location.type <- 'geocodes.bad'
    DT.2$street.num.temp <- NULL
    #DT.cols[!(DT.cols %in% names(DT.2))]
    # add onto the bottom of DT
    if (nrow(DT)>0){
      DT.address.ids <- unique(DT$address.id)
      DT.2 <- DT.2[!(address.id %in% DT.address.ids)]
      l.DT <- list(DT, DT.2)
      DT <- rbindlist(l.DT, use.names=TRUE, fill=TRUE)
      #   print(nrow(DT))
      DT <- DT[, DT.cols, with=FALSE]
    } else {
      DT <- DT.2
    }
  }
  return(DT)
}
#' @title funImport.location.geocode
#'
#' @description string matches address (x) to shapefiles address
#' @param locs table showing what shapes the address.ids are associated with
#' @param address data to be assigned
#' @param pkg.data.root source dropbox data
#' @param api.key google api key
#' @param l.study.extent cities, zips, states
#' @param location.source source data mmed.1348998
#' @keywords assign location import
#' @export
#' @import stringr
#'     data.table
#'     methods.string
#'     points
funImport.location.geocode <- function(locs, address, pkg.data.root, api.key, l.study.extent, location.source){
  pkg.name <- location.id <- street.num <- address.num.low <- address.num.hi <- address.address.id <- NULL
  street.num.low <- street.num.hi <- locs.address.id <- NULL
  geocode.n <- location.type <- city <- street <- NULL
  DT.geo <- match.threshold <- pkg.name <- cityStreetZip <- pkg.data.root <- NULL
  address.id <- NULL
  dt.pkg.data <- pkg.data.paths::dt(path.root = pkg.data.root)
  points.address <- points::address(dt.pkg.data[pkg.name=='points']$sys.path)
  states.abbrev <- names(table(address$state))[table(address$state)/nrow(address) > 0.5 & names(table(address$state)) != ""]
  funCheck.geocode.ids <- function(locs, address){
    pkg.name <- location.type <- address.id <- address.address.id <- address.num.low <- NULL
    address.num.hik <- street.num.range <- street.num.points.address <- NULL
    points.address <- points::address(dt.pkg.data[pkg.name=='points']$sys.path)
    check.ids <- list()
    locs.points <- locs[location.type=='points']
    check.ids$bad <- sort(locs[location.type=='geocodes.bad', address.id])
    # Check for addresses with street num ranges
    x.locs <- locs.points[,.(address.id, location.id)]
    x.points.address <- points.address[, .(location.id, street.num.points.address = street.num)]
    setkey(x.locs, location.id)
    setkey(x.points.address, location.id)
    x.locs <- x.points.address[x.locs]
    x.locs$locs.address.id <- x.locs$address.id
    x.address <- address[,.(address.id, address.address.id = address.id, address.num.low = street.num.low, address.num.hi = street.num.hi)]
    setkey(x.address, address.id)
    setkey(x.locs, address.id)
    x <- x.locs[x.address]
    setkey(x, address.id, street.num.points.address)
    x <- unique(x)
    x$street.num.range <- FALSE
    x <- x[address.num.low != address.num.hi, street.num.range:=TRUE]
    x$geocode.n <- 0
    x <- x[!(is.na(locs.address.id)), geocode.n := as.numeric(.N), by=address.id]
    check.ids$noRange <- unique(x[!(is.na(locs.address.id)) & street.num.range & geocode.n < 2 & !(address.id %in% check.ids$bad)]$address.id)
    check.ids$noCode <- unique(x[is.na(locs.address.id) & !(address.id %in% check.ids$bad)]$address.id)
    address.ids.missing <- unique(c(check.ids$noCode, check.ids$noRange))
    address.ids.missing <- address.ids.missing[!(address.ids.missing %in% check.ids$bad)]
    address.ids.skip <- unique(c(address.ids.missing, check.ids$bad))
    check.ids$missing <- address[address.id %in% address.ids.missing]
    check.ids$coded <- unique(sort(address[!(address.id %in% address.ids.skip)]$address.id))
    cat(paste('----------------------'),
        paste('gecode possible:      ', nrow(address)),
        paste('----------------------'),
        paste('complete:             ', length(unique(check.ids$coded))),
        paste('geocode missing all:  ', length(unique(check.ids$noCode))),
        paste('geocode missing one:  ', length(check.ids$noRange)),
        paste('bad:                  ', length(unique(check.ids$bad))),
        paste('-------------------------'),
        c('', ''),
        sep = '\n')
    return(check.ids)
  }
  # Geocode missing 
  check.ids <- funCheck.geocode.ids(locs, address)
  # Initialize empty table
  address.ids.missing <- check.ids$missing$address.id
  location.ids.points <- locs[location.type=='points' & address.id %in% address.ids.missing]
  i.missing <- 0
  if (nrow(check.ids$missing) > 0){
    for (missing.id in address.ids.missing){
      i.missing <- i.missing + 1
      DT.geo.point <- check.ids$missing[address.id == missing.id]
      print(paste('geocoding >> address.id:', DT.geo.point$address.id, '| street:', DT.geo.point$street, ' Number ', i.missing, 'of', length(address.ids.missing)))
      if (!(is.na(DT.geo.point$street.num))){
        DT.geo.point.unique <- DT.geo.point$street.num.low==DT.geo.point$street.num.hi
        # Only one street number
        if (DT.geo.point.unique){
          DT.geos <- geocode::geocode(pkg.data.root, DT.geo.point, api.key, location.source,  l.study.extent)
        } else { # Range of street numbers
          DT.geo.list <- list()
          DT.geo.low <- copy(DT.geo.point[, street.num:=street.num.low])
          DT.geo.high <- copy(DT.geo.point[, street.num:=street.num.hi])
          DT.geo.list$low <- geocode::geocode(pkg.data.root, DT.geo.low, api.key, location.source, l.study.extent)
          DT.geo.list$high <- geocode::geocode(pkg.data.root, DT.geo.high, api.key, location.source, l.study.extent)
          
          num.range <- unlist(DT.geo.point[,.(street.num.low, street.num.hi)])
          nums <- seq(num.range[1], num.range[2])
          nums <- nums[!(nums %in% num.range)]
          nums.n <- length(nums)
          nums.inds <- unique(sample(1:nums.n, size = 10, replace=TRUE))
          nums <- nums[nums.inds]
          loc.match <- FALSE
          iter <- 0
          i.hi <- length(nums)
          i.low <- 1
          end.loc <- c(0,0)
          while (iter <= length(nums) & loc.match==FALSE){
            iter <- iter+1
            start.loc <- nums[iter]
            if(iter %% 2 == 0){
              i <- i.hi
              i.hi <- i.hi - 1
            } else {
              i <- i.low
              i.low <- i.low + 1
            }
            DT.temp <- DT.geo.point[, street.num:=nums[i]]
            DT.geo.list[[iter+2]] <- geocode::geocode(pkg.data.root, DT.temp, api.key, location.source, l.study.extent)
            end.loc <- nums[i]
            if (iter==1){
              loc.match == FALSE
            } else {
              loc.match <- (identical(end.loc, start.loc) | DT.geo$match.rank > 2)
            }
          }
          DT.geos <- rbindlist(DT.geo.list, use.names=TRUE, fill=TRUE)
        }
      } else {
        geocodes.bad.address <- geocode::geocodes.bad.address(geocodes.bad.address.new = DT.geo.point, location.source)
      }
    }
    locs <- funUpdate.locs(address, match.threshold, pkg.data.root)
    check.ids <- funCheck.geocode.ids(locs, address)
  }
  return(locs)
}
#' @title funImport.location.intersect
#'
#' @description intersect points to shapes
#' @param locs assignment table
#' @param address to be looked up
#' @param pkg.data.root dropbox data root dir
#' @param location.source data source
#' @keywords assign location import
#' @export
#' @import stringr
#'     data.table
#'     methods.string
#'     methods.shapes
#'     points
#'     parcels
#'     pkg.data.paths
#'     sp
#'     tidyr
#'     dplyr
funImport.location.intersect <- function(locs, address, pkg.data.root, location.source){
  pkg.name <- pkg.root <- point.id <- outline.id <- parcel.id <-address.id <- NULL
  outlines.address <- parcels.address <- state <- zip <- NULL
  location.id <- location.type <- cityStateZip <- city <- match.threshold <- NULL
  l.address <- fun.l.address(pkg.data.root)
  dt.pkg.data <- pkg.data.paths::dt(path.root = pkg.data.root)
  shapes.parcels <- parcels::shapes(dt.pkg.data[pkg.name=='parcels']$pkg.root[1]) 
  shapes.outlines <- outlines::shapes(dt.pkg.data[pkg.name=='outlines']$pkg.root[1])
  funCheck.ids <- function(locs, address, pkg.data.root){
    l.address <- fun.l.address(pkg.data.root)
    setkey(locs, address.id, location.id, location.type)
    locs <- unique(locs)
    check.ids <- list()
    points.address <- l.address$points
    locs.points <- locs[location.type=='points'][, .(address.id, point.id = location.id)]
    locs.outlines <- locs[location.type=='outlines'][, .(address.id, outline.id = location.id)]
    locs.parcels <- locs[location.type=='parcels'][, .(address.id, parcel.id = location.id)]
    setkey(locs.parcels, address.id)
    setkey(locs.outlines, address.id)
    join.1 <- locs.parcels[locs.points]
    locs.spread <- locs.outlines[join.1]
    
    cat(paste('Address', length(unique(locs$address.id))), sep='\n')
    
    missing <- list()
    missing$parcel <- locs.spread[is.na(parcel.id)]
    missing$outline <- locs.spread[is.na(outline.id)]
    missing$point <- locs.spread[is.na(point.id)]
    cat('Missing:', sep='\n')
    out <- unlist(lapply(missing, function(x) length(unique(x$address.id))))
    out <- data.table(type=names(out), n.missing=out)
    print(out)
    
    no.match <- list()
    no.match$parcel <- locs.spread[parcel.id =='no.match']
    no.match$outline <- locs.spread[outline.id =='no.match']
    no.match$point <- locs.spread[point.id =='no.match']
    cat('No.match:', sep='\n')
    out <- unlist(lapply(no.match, function(x) length(unique(x$address.id))))
    out <- data.table(type=names(out), no.match=out)
    print(out)
    return(locs.spread)
  }
  # Global Overlays
  cat('Start Stats', sep='\n')
  locs.spread <- funCheck.ids(locs, address, pkg.data.root)
  cat(' ', sep='\n')
  setnames(l.address$points, 'location.id', 'point.id')
  setkey(l.address$points, point.id)
  
  # Overlay parcels
  locs.parcel <- unique(locs.spread[is.na(parcel.id)][,.(point.id)])
  setkey(locs.parcel, point.id)
  locs.parcel <- l.address$points[locs.parcel]
  if (nrow(locs.parcel)>0){
    DT.geocoded.shapes <- methods.shapes::shapes.coords2points(locs.parcel)
    # Step 3: Update [outlines.address]
    print('Intersecting parcels...')
    DT.points.parcels <- sp::over(DT.geocoded.shapes, shapes.parcels)
    parcel.ids <-  DT.points.parcels$parcel.id
    DT.geo.parcels.intersect <- as.data.table(cbind(parcel.id=parcel.ids, locs.parcel))
    DT.geo.parcels <- DT.geo.parcels.intersect[is.na(parcel.id), parcel.id := 'no.match']
    DT.geo.parcels.address.new <-  DT.geo.parcels
    setnames(DT.geo.parcels.address.new, 'parcel.id', 'location.id')
    col.names <- names(parcels.address)
    col.names <- col.names[!(col.names %in% c('area','location.source', 'location.street.direction', 'location.street.num'))]
    if (!('cityStateZip' %in% names(DT.geo.parcels.address.new))){
      DT.geo.parcels.address.new <- DT.geo.parcels.address.new[, cityStateZip := paste0(city, ' ', state, ', ',zip)]
    }
    DT.geo.parcels.address.new <- DT.geo.parcels.address.new[, (col.names), with=FALSE]
    l.address$parcels <- parcels::address.update(pkg.data.root, l.address$parcels, DT.geo.parcels.address.new)
    locs <- funUpdate.locs(address, match.threshold, pkg.data.root)
  }
  
  # Overlay outlines
  locs.outline <- unique(locs.spread[is.na(outline.id)][,.(point.id)])
  setkey(locs.outline, point.id)
  locs.outline <- l.address$points[locs.outline]
  if (nrow(locs.outline)>0){
    DT.geocoded.shapes <- methods.shapes::shapes.coords2points(locs.outline)
    # Step 3: Update [outlines.address]
    print('Intersecting outlines...')
    DT.points.outlines <- sp::over(DT.geocoded.shapes, shapes.outlines)
    outline.ids <-  DT.points.outlines$outline.id
    DT.geo.outlines.intersect <- as.data.table(cbind(outline.id=outline.ids, locs.outline))
    DT.geo.outlines <- DT.geo.outlines.intersect[is.na(outline.id), outline.id := 'no.match']
    DT.geo.outlines.address.new <-  DT.geo.outlines
    setnames(DT.geo.outlines.address.new, 'outline.id', 'location.id')
    col.names <- names(outlines.address)
    col.names <- col.names[!(col.names %in% c('area','location.source', 'location.street.direction', 'location.street.num'))]
    if (!('cityStateZip' %in% names(DT.geo.outlines.address.new))){
      DT.geo.outlines.address.new <- DT.geo.outlines.address.new[, cityStateZip := paste0(city, ' ', state, ', ',zip)]
    }
    DT.geo.outlines.address.new <- DT.geo.outlines.address.new[, (col.names), with=FALSE]
    l.address$outlines <- outlines::address.update(pkg.data.root, l.address$outlines, DT.geo.outlines.address.new)
    locs <- funUpdate.locs(address, match.threshold, pkg.data.root)
  }
  check.ids <- funCheck.ids(locs, address, pkg.data.root)
  return(locs)
}
#' @title funUpdate.locs
#'
#' @description Assigns address to locations in last stage of import
#' @param address data.table with all exploded address fields
#' @param match.threshold string match quality
#' @param pkg.data.root package path data
#' @keywords parcels clean update locations
#' @export
#' @import stringr
#'     data.table
#'     stringdist
#'     methods.string
#'     parcels
#'     outlines
funUpdate.locs <- function(address, match.threshold, pkg.data.root){
  street <- address.id <- address.id <- address.num.hi <- address.num.low <- state <- NULL
  location.id <- NULL
  totals <- nrow(address)
  names(totals) <- 'possible'
  l.address <- fun.l.address(pkg.data.root)
  l.locations <- list(parcels=list(type='parcels', address=l.address$parcels, match.threshold=match.threshold),
                      points=list(type='points', address=l.address$points, match.threshold=match.threshold),
                      outlines=list(type='outlines', address=l.address$outlines, match.threshold=match.threshold),
                      geocodes.bad=list(type='geocodes.bad', address = l.address$geocodes.bad, match.threshold=0.5))
  # Join Data to existing address -> locations
  l.locs <- sapply(l.locations, function(l.location) funImport.location.address(l.location, address), USE.NAMES=TRUE, simplify = FALSE)
  # Make sure of no overly zealous matches on geocodes.bad
  l.locs.ids <- which(lapply(l.locs, nrow)>0)
  l.locs.ids.sub <- l.locs.ids[!(l.locs.ids%in%1)]
  addresses.good <- unique(unlist(sapply(l.locs[l.locs.ids.sub], '[[', 1)))
  temp <- l.locs$geocodes.bad[!(address.id %in% addresses.good)]
  l.locs$geocodes.bad 
  diagnose <- list(n.assigned = c(totals,sapply(l.locs[l.locs.ids], function(x) length(unique(x[location.id != 'no.match', address.id])))))
  cat('-------------------', sep='\n')
  cat(sapply(diagnose, function(y) paste0('Address assigned to ',str_pad(names(y), 13, 'right'), ' : ', y)), sep='\n')
  cat('-------------------', sep='\n')
  locs <- rbindlist(l.locs, use.names = TRUE)
  locs <- locs[, c('address.id', 'location.id', 'location.type'), with=FALSE]
  ## Debug check
  address.ids <- unique(address$address.id)
  address.ids.n <- length(address.ids)
  locs.address.ids <- unique(locs$address.id)
  locs.address.ids.n <- length(locs.address.ids)
  debug.address <- c(paste('Addresses missing:   ', address.ids.n - locs.address.ids.n))
  cat(debug.address, sep='\n')
  missing.addresses <- address[!(address.ids %in% locs.address.ids), .(address.id, street)]
  l.missing <- split(missing.addresses, seq(nrow(missing.addresses)))
  if ((address.ids.n - locs.address.ids.n)!=0){
    cat(sapply(l.missing, function(x) paste('address.id: ', x$address.id, '| street:', x$street)), sep='\n')
  }
  return(locs)
}
#' @title fun.l.address
#'
#' @description Assigns a list of addresses
#' @param pkg.data.root dropbox root
#' @keywords parcels, clean, update, locations, list
#' @export
#' @import stringr
#'     data.table
#'     methods.string
#'     parcels
#'     outlines
#'     geocode
#'     points
fun.l.address <- function(pkg.data.root){
  pkg.name <- NULL
  dt.pkg.data <- pkg.data.paths::dt(pkg.data.root)
  parcels.address <- parcels::address(dt.pkg.data[pkg.name=='parcels']$pkg.root[1])
  outlines.address <- outlines::address(dt.pkg.data[pkg.name=='outlines']$pkg.root[1])
  points.address <- points::address(dt.pkg.data[pkg.name=='points']$sys.path)
  geocodes.bad.address <- geocode::geocodes.bad.address(pkg.data.root)
  return(list(parcels=parcels.address, points=points.address, outlines=outlines.address, geocodes.bad=geocodes.bad.address))
}